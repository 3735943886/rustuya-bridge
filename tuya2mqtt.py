# -*- coding: utf-8 -*-
import argparse
import sys
import json
from datetime import datetime
import asyncio
import tinytuya
import aiomqtt
import os
import resource
import signal
import time
import traceback
import logging
import functools
import re
from string import Template
from contextlib import asynccontextmanager

if hasattr(tinytuya, 'DeviceAsync'):
  DeviceAsync = tinytuya.DeviceAsync
else:
  import tinytuya_async
  DeviceAsync = tinytuya_async.DeviceAsync

class DeviceOperationGuard:
    def __init__(self):
        self._adding_count = 0
        self._deleting_count = 0

    @asynccontextmanager
    async def adding(self, last_callback=None):
        if self._deleting_count > 0:
            raise RuntimeError("Deletion is currently in progress.")
        self._adding_count += 1
        try:
            yield
        finally:
            self._adding_count -= 1
            if self._adding_count == 0 and last_callback:
                try:
                    await last_callback()
                except Exception as e:
                    logging.error(f"Callback failed in guard: {e}")

    @asynccontextmanager
    async def deleting(self, last_callback=None):
        if self._adding_count > 0:
            raise RuntimeError("Adding is currently in progress.")
        self._deleting_count += 1
        try:
            yield
        finally:
            self._deleting_count -= 1
            if self._deleting_count == 0 and last_callback:
                try:
                    await last_callback()
                except Exception as e:
                    logging.error(f"Callback failed in guard: {e}")


class MqttLogHandler(logging.Handler):
    def __init__(self, bridge_instance):
        super().__init__()
        self.bridge = bridge_instance
        self.log_queue = asyncio.Queue(maxsize=1000)
        self.bridge.log_publisher_task = None
        self.publisher_lock = asyncio.Lock()
        self.level_to_topic_map = {
            logging.DEBUG: 'debug',
            logging.INFO: 'info',
            logging.WARNING: 'warning',
            logging.ERROR: 'error',
            logging.CRITICAL: 'critical',
        }

    def emit(self, record):
        if self.bridge.log_publisher_task is None:
            self.bridge.log_publisher_task = self.bridge.task_creator(self._log_publisher())
        try:
            message = record
            message = self.format(record)
            category = self.level_to_topic_map.get(record.levelno, 'info')
            self.log_queue.put_nowait((category, message))
        except asyncio.QueueFull:
            sys.stderr.write(f"MQTT_LOG_QUEUE_FULL: Log message dropped. | {message}\n")
        except Exception as e:
            sys.stderr.write(f"MQTT_LOG_FAIL: {e} | {message}\n")

    async def _log_publisher(self):
        async with self.publisher_lock:
            while True:
                category, message = await self.log_queue.get()
                try:
                    if self.bridge.mqtt_publisher:
                        topic = self.bridge.settings['topic']['log'][category]
                        await self.bridge.mqtt_publisher(topic=topic, payload=message)
                    else:
                        sys.stderr.write(f"{message}\n")
                except Exception as e:
                    sys.stderr.write(f"MQTT_LOG_FAIL: {e} | {message}\n")
                finally:
                    self.log_queue.task_done()


class Tuya2MQTTBridge:
    def __init__(self, **kwargs):
        # --- Initialization ---
        self._real_task_creator = kwargs.pop('task_creator', asyncio.create_task)
        self._background_tasks = set()
        self.task_creator = self._safe_task_creator

        self.log_level = kwargs.pop('log_level', 'WARNING')
        self.logger = kwargs.pop('logger', None)
        self.mqtt_publisher = kwargs.pop('mqtt_publisher', None)
        self.task_creator = kwargs.pop('task_creator', asyncio.create_task)
        self.root_topic = kwargs.pop('root_topic', 'tuya2mqtt')
        foreground = kwargs.pop('foreground', False)

        self.topic_done = False
        self.snapshot_done = False
        self._snapshot_pending = False
        self.is_shutting_down = False
        self.shutdown_event = asyncio.Event()
        self.op_guard = DeviceOperationGuard()

        inflow_topic = f"{self.root_topic}/intro"
        outflow_topic = f"{self.root_topic}/extra"
        self.settings = {
            'broker': {
                'hostname': 'localhost',
                'port': 1883,
            },
            'topic': {
                'subscribe': {
                    'add': f"{inflow_topic}/device/add",
                    'delete': f"{inflow_topic}/device/delete",
                    'set': f"{inflow_topic}/device/set",
                    'get': f"{inflow_topic}/device/get",
                    'command': f"{inflow_topic}/device/command",
                    'query': f"{inflow_topic}/daemon/query",
                },
                'publish': {
                    'active': f"{outflow_topic}/device/active",
                    'passive': f"{outflow_topic}/device/passive",
                    'message': f"{outflow_topic}/device/message",
                    'daemon': f"{outflow_topic}/daemon/status",
                    'snapshot': f"{outflow_topic}/daemon/snapshot",
                },
                'log': {
                    'debug': f"{self.root_topic}/log/debug",
                    'info': f"{self.root_topic}/log/info",
                    'warning': f"{self.root_topic}/log/warning",
                    'error': f"{self.root_topic}/log/error",
                    'critical': f"{self.root_topic}/log/critical",
                },
            },
            'format': {
                'publish': '{"id": $id, "name": $name, "data": $dps}',
                'log': '%(asctime)s - %(levelname)s - %(message)s',
            },
            'daemon': {
                'subdevice_add_retries': 10,
                'retry_delay_seconds': 5,
                'max_retry_delay_seconds': 320,
                'heartbeat_interval': 10,
                'instance_lock_timeout': 0.2,
            },
        }
        self.daemon_stat = {
            'version': '1.2.25',
            'start_time': datetime.now(),
        }
        self.tuya_state = {
            'device': {
                'id': {},
                'name': {},
            },
        }
        # Other configuration
        self._load_conf(kwargs)

        # Settup logger if not set
        self.log_format = self.settings.get('format', {}).get('log')
        if self.logger is None:
            self._setup_logging(foreground)
        if self.log_level == logging.DEBUG or self.log_level == 'DEBUG':
            tinytuya.set_debug(True)
            print("TinyTuya debugging enabled.")

        # Increase file descriptor limit if possible
        try:
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            if soft < hard:
                resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
        except (ValueError, OSError) as e:
            print(f"Could not set rlimit: {e}")

    # --- Task creator ---
    def _safe_task_creator(self, coro):
        task = self._real_task_creator(coro)
        if hasattr(task, 'add_done_callback'):
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
        return task

    # --- Logging ---
    def _setup_logging(self, foreground):
        self.logger = logging.getLogger('Tuya2MQTTBridge')
        try:
            self.logger.setLevel(self.log_level)
        except (ValueError, AttributeError):
            self.logger.setLevel(logging.WARNING)
        self.logger.propagate = False
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        formatter = logging.Formatter(self.log_format)
        if foreground:
            handler = logging.StreamHandler(sys.stdout)
        else:
            handler = MqttLogHandler(self)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    # --- Configuration ---
    def _load_conf(self, config):
        def deep_update(d, u):
            for k, v in u.items():
                if isinstance(v, dict) and isinstance(d.get(k), dict):
                    deep_update(d[k], v)
                else:
                    d[k] = v
            return d
        try:
            if config and isinstance(config, dict):
                deep_update(self.settings, config)
        except Exception as e:
            if self.logger:
                self.logger.error(f"CONFIG_ERROR: Could not parse config: {e}")
            else:
                print(f"CONFIG_ERROR: Could not parse config: {e}")

    @staticmethod
    @functools.lru_cache(maxsize=16)
    def _compile_topic_regex(config_topic):
        parts = config_topic.split('/')
        regex_parts = []
        for part in parts:
            if part.startswith('$') and len(part) > 1:
                var_name = part[1:]
                regex_parts.append(f'(?P<{var_name}>[^/]+)')
            else:
                regex_parts.append(re.escape(part))
        return re.compile('^' + '/'.join(regex_parts) + '$')

    @staticmethod
    def _value_converter(value):
        if isinstance(value, (bool, int, float)):
            return value
        v = str(value).strip()
        v_lower = v.lower()
        if v_lower == 'true':
            return True
        elif v_lower == 'false':
            return False

        try:
            return int(v)
        except ValueError:
            pass
        try:
            return float(v)
        except ValueError:
            return value

    @staticmethod
    def match_topic(incoming_topic, config_topic):
        if '$' not in config_topic:
            return {'_': None} if config_topic == incoming_topic else None
        pattern = Tuya2MQTTBridge._compile_topic_regex(config_topic)
        m = re.match(pattern, incoming_topic)
        if m:
            result = m.groupdict()
            for k, v in result.items():
                result[k] = Tuya2MQTTBridge._value_converter(v)
            return result
        return None

    # --- Core Logic ---
    async def recv_topic(self, msg):
        topic_str = str(msg.topic)
        topic_map = self.settings['topic']['subscribe']
        payload_str = None
        payloads = None
        try:
            if isinstance(msg.payload, bytes):
                payload_str = msg.payload.decode('utf-8') or None
            else:
                payload_str = msg.payload or None
        except UnicodeDecodeError:
            payload_str = msg.payload or None
        self.logger.info(f"Topic: {topic_str}, Payload: {payload_str}")
        try:
            payloads = json.loads(payload_str)
        except (json.decoder.JSONDecodeError, TypeError):
            payloads = payload_str
        if not isinstance(payloads, list):
            payloads = [payloads]

        # --- TOPIC INFORMATION ---
        if topic_str == self.root_topic:
            if not self.topic_done:
                self.topic_done = True
                if self.mqtt:
                    await self.mqtt.unsubscribe(self.root_topic)
                await self.mqtt_publisher(topic=self.root_topic, payload=json.dumps(self.settings['topic']), retain=True)

        # --- SNAPSHOT ---
        elif topic_str == self.settings['topic']['publish']['snapshot']:
            if not self.snapshot_done:
                self.snapshot_done = True
                if self.mqtt:
                    await self.mqtt.unsubscribe(self.settings['topic']['publish']['snapshot'])
                await self.mqtt_publisher(topic=self.settings['topic']['subscribe']['add'], payload=json.dumps(payloads))

        # --- ADD DEVICE ---
        elif _dps := self.match_topic(topic_str, topic_map['add']):
            async def _add_task(payload, dps):
                if not isinstance(payload, dict):
                    payload = {'_payload': payload} if payload != None and payload != '' else {}
                payload.update(dps)
                task = None
                added = None
                parent = None
                try:
                    # Subdevice
                    if 'parent' in payload and 'node_id' in payload and payload['parent'] and payload['node_id']:
                        if 'id' not in payload:
                            payload['id'] = payload['node_id']
                            if payload.get('name'):
                                payload['id'] = f"{payload['id']}_{payload['name']}"
                        for _ in range(self.settings['daemon']['subdevice_add_retries']):
                            if payload['parent'] in self.tuya_state['device']['id']:
                                if self.tuya_state['device']['id'][payload['parent']].get('status') == 'online':
                                    parent = self.tuya_state['device']['id'][payload['parent']].get('device')
                            if parent:
                                added = DeviceAsync(dev_id=payload['id'], cid=payload['node_id'], parent=parent)
                                self.tuya_state['device']['id'][payload['id']] = {'device': added, 'parent': payload['parent']}
                                keys_to_save = ['id', 'node_id', 'parent', 'name']
                                self.tuya_state['device']['id'][payload['id']]['payload'] = {k: payload[k] for k in keys_to_save if k in payload}
                                self.tuya_state['device']['id'][payload['id']]['last_seen'] = None
                                if payload['node_id'] not in self.tuya_state['device']['id'][payload['parent']]['children']:
                                    self.tuya_state['device']['id'][payload['parent']]['children'][payload['node_id']] = payload['id']
                                self._add_device_name_mapping(payload['id'], payload.get('name'))
                                self.logger.info(f"Subdevice added: {payload['id']}")
                                break
                            self.logger.info(f"Parent not found, retrying: {payload['id']}")
                            await asyncio.sleep(self.settings['daemon']['retry_delay_seconds'])
                        if added is None:
                            self.logger.error(f"Parent not found, fail to add subdevice: {payload['id']}")
                    # WiFi Device
                    elif 'key' in payload and 'ip' in payload and 'version' in payload:
                        duplicated = False
                        for device_info in self.tuya_state['device']['id'].values():
                            if 'ip' in device_info['payload'] and device_info['payload']['ip'] == payload['ip']:
                                duplicated = True
                                break
                        if 'id' in payload:
                            if payload['id'] in self.tuya_state['device']['id']:
                                duplicated = True
                        else:
                            payload['id'] = payload['ip']
                            if payload.get('name'):
                                payload['id'] = f"{payload['id']}_{payload['name']}"
                        if duplicated:
                            self.logger.warning(f"Already added, ignoring: {payload['id']}")
                        else:
                            self.tuya_state['device']['id'][payload['id']] = {}
                            keys_to_save = ['id', 'ip', 'key', 'version', 'name']
                            self.tuya_state['device']['id'][payload['id']]['payload'] = {k: payload[k] for k in keys_to_save if k in payload}
                            self.tuya_state['device']['id'][payload['id']]['status'] = 'connecting'
                            self.tuya_state['device']['id'][payload['id']]['last_seen'] = None
                            task = self.task_creator(self._tuya_receiver(payload))
                            self.tuya_state['device']['id'][payload['id']]['task'] = task
                            self.tuya_state['device']['id'][payload['id']]['children'] = {}
                            self._add_device_name_mapping(payload['id'], payload.get('name'))
                            self.logger.info(f"Device added and listener task created: {payload['id']}")
                except Exception:
                    self.logger.error(traceback.format_exc())
                    if added:
                        try:
                            added.close()
                        except:
                            pass
            try:
                async with self.op_guard.adding(last_callback=lambda: self._snapshot(True)):
                    payloads.sort(key=lambda x: ('node_id' in x or 'parent' in x))
                    _tasks = []
                    for _payload in payloads:
                        _tasks.append(self.task_creator(_add_task(_payload, _dps)))
                    await asyncio.gather(*_tasks)
            except Exception:
                self.logger.error(traceback.format_exc())

        # --- DELETE DEVICE ---
        elif _dps := self.match_topic(topic_str, topic_map['delete']):
            delete_task = []
            try:
                async with self.op_guard.deleting(last_callback=lambda: self._snapshot(True)):
                    for payload in payloads:
                        if not isinstance(payload, dict):
                            payload = {'_payload': payload} if payload != None and payload != '' else {}
                        payload.update(_dps)
                        try:
                            target_ids = self._get_target_ids(payload)
                            for device_id in target_ids:
                                if device_id in self.tuya_state['device']['id']:
                                    device_info = self.tuya_state['device']['id'][device_id]
                                    if 'task' in device_info:
                                        delete_task.append(device_info['task'])
                                        device_info['task'].cancel()
                                    else: # Subdevice
                                        try:
                                            parent_id = device_info.get('parent')
                                            if parent_id and parent_id in self.tuya_state['device']['id']:
                                                self.tuya_state['device']['id'][parent_id]['children'] = {key: value for key, value in self.tuya_state['device']['id'][parent_id]['children'].items() if value != device_id}
                                        except (KeyError, ValueError):
                                            pass
                                        self._cleanup_device(device_id)
                                    self.logger.info(f"Device deleted: {device_id}")
                        except Exception:
                            self.logger.error(traceback.format_exc())
                    await asyncio.gather(*delete_task, return_exceptions=True)
            except Exception:
                self.logger.error(traceback.format_exc())

        # --- GET/SET/COMMAND ---
        elif _dps := (self.match_topic(topic_str, topic_map['get']) or self.match_topic(topic_str, topic_map['set']) or self.match_topic(topic_str, topic_map['command'])):
            for payload in payloads:
                _cmd_args = []
                _cmd_kwargs = {}
                _cmd_name = ''
                _break_flag = False
                try:
                    if isinstance(payload, dict):
                        if 'args' in payload or 'kwargs' in payload or 'data' in payload:
                            _data_value = payload.get('data')
                            if isinstance(_data_value, dict):
                                _cmd_kwargs.update(_data_value)
                            elif isinstance(_data_value, list):
                                _cmd_args.extend(_data_value)
                            elif _data_value is not None:
                                _cmd_args.append(_data_value)
                            _args_value = payload.get('args')
                            if isinstance(_args_value, list):
                                _cmd_args.extend(_args_value)
                            elif _args_value is not None:
                                _cmd_args.append(_args_value)
                            _kwargs_value = payload.get('kwargs', {})
                            if isinstance(_kwargs_value, dict):
                                _cmd_kwargs.update(payload.get('kwargs', {}))
                        else:
                            _cmd_kwargs = {k: v for k, v in payload.items() if k not in ('id', 'name', 'command')}
                        _dps.update(payload)
                    elif isinstance(payload, list):
                        _cmd_args = payload
                    elif payload:
                        _cmd_args = payloads
                        _break_flag = True
                    target_devices = self._get_target_devices(_dps)
                    if self.match_topic(topic_str, topic_map['get']):
                        _cmd_name = 'status'
                    elif self.match_topic(topic_str, topic_map['set']):
                        if 'dp' in _dps or 'index' in _dps or 'value' in _dps:
                            if _cmd_kwargs:
                                if 'index' not in _cmd_kwargs:
                                    _cmd_kwargs['index'] = _dps.get('index') or _dps.get('dp')
                                if 'value' not in _cmd_kwargs:
                                    _cmd_kwargs['value'] = _dps.get('value')
                            else:
                                if len(_cmd_args) < 2:
                                    _cmd_args.insert(0, _dps.get('index') or _dps.get('dp'))
                                if len(_cmd_args) < 2:
                                    _cmd_args.insert(1, _dps.get('value'))
                            _cmd_name = 'set_value'
                        elif _cmd_kwargs:
                            _cmd_name = 'set_multiple_values'
                    elif self.match_topic(topic_str, topic_map['command']):
                        _cmd_name = _dps.get('command', '')
                    if not _cmd_name or _cmd_name.startswith('_'):
                        self.logger.warning(f"Attempt to call an invalid or private command ('{_cmd_name}'). Ignoring.")
                        break
                    for device in target_devices:
                        try:
                            if _cmd_name == 'set_multiple_values' and not _cmd_args and _cmd_kwargs:
                                # set_multiple_values accepts positional arg as index_value_dict
                                _cmd_args = [_cmd_kwargs]
                                _cmd_kwargs = {}
                            self.logger.info('Executing {_cmd_name}({_cmd_args}{_cmd_kwargs}) for device {deviceid}'.format(_cmd_name=_cmd_name, _cmd_args=_cmd_args or '', _cmd_kwargs=_cmd_kwargs or '', deviceid=device.id))
                            _cmd_func = getattr(device, _cmd_name)
                            if asyncio.iscoroutinefunction(_cmd_func):
                                _result = await _cmd_func(*_cmd_args, **_cmd_kwargs)
                                device.tuya2mqtt_last_sent_time = time.monotonic()
                            else:
                                _result = _cmd_func(*_cmd_args, **_cmd_kwargs)
                            if _result:
                                if self.mqtt_publisher:
                                    _publish_id = _dps['id']
                                    _publish_name = self.tuya_state['device']['id'].get(_dps['id'], {}).get('name', '')
                                    _publish_payload = {'id': _publish_id}
                                    if _publish_name: _publish_payload['name'] = _publish_name
                                    _publish_payload['result'] = _result
                                    _topic_template = Template(self.settings['topic']['publish']['message'])
                                    _topic_final = _topic_template.safe_substitute(id=_publish_id, name=_publish_name)
                                    await self.mqtt_publisher(
                                        topic=_topic_final,
                                        payload=json.dumps(_publish_payload, default=lambda _obj: str(_obj), ensure_ascii=False))
                                else:
                                    self.logger.info(json.dumps(_publish_payload, default=lambda _obj: str(_obj), ensure_ascii=False))
                        except AttributeError:
                            self.logger.error(f"Device {device.id} has no command named '{_cmd_name}'.")
                        except TypeError as e:
                            self.logger.error(f"Invalid arguments for command '{_cmd_name}' on device {device.id}: {e}")
                except Exception:
                    self.logger.error(traceback.format_exc())
                if _break_flag:
                    break

        # --- QUERY DAEMON ---
        elif topic_str == topic_map['query']:
            for payload in payloads:
                if payload:
                    if 'stop' in payload or 'reset' in payload:
                        await self.terminator('stop' in payload)
                    elif 'status' in payload:
                        await self._publish_daemon_status(payload)

    # --- Publish Snapshot ---
    async def _snapshot(self, details=False, debounce=0):
        if self._snapshot_pending:
            return
        self._snapshot_pending = True
        if debounce: await asyncio.sleep(debounce)
        try:
            self.logger.info('Publishing snapshot...')
            device_snapshot = self._get_device_payloads(details)
            await self.mqtt_publisher(
                topic=self.settings['topic']['publish']['snapshot'],
                payload=json.dumps(device_snapshot, ensure_ascii=False),
                retain=True)
        finally:
            self._snapshot_pending = False

    # --- Terminator ---
    async def terminator(self, quit_program):
        if self.is_shutting_down or self.shutdown_event.is_set():
            return
        self.is_shutting_down = True
        tasks_to_cancel = []
        for device_id in list(self.tuya_state['device']['id'].keys()):
            device_info = self.tuya_state['device']['id'].get(device_id, {})
            if 'task' in device_info:
                tasks_to_cancel.append(device_info['task'])
        if quit_program:
            await self.mqtt_publisher(topic=self.root_topic, retain=True)
            if hasattr(self, 'log_publisher_task'):
                if self.log_publisher_task:
                    tasks_to_cancel.append(self.log_publisher_task)
        for task in tasks_to_cancel:
            task.cancel()
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        self.logger.info(f"All device tasks cancelled. Exiting: {quit_program}")
        if quit_program:
            self.shutdown_event.set()
        else:
            await self._snapshot()
            self.is_shutting_down = False

    # --- Helper Functions ---
    def _add_device_name_mapping(self, device_id, name):
        """Adds a device ID to the name->ID mapping."""
        if not name:
            return
        self.tuya_state['device']['id'][device_id]['name'] = name
        if name not in self.tuya_state['device']['name']:
            self.tuya_state['device']['name'][name] = []
        if device_id not in self.tuya_state['device']['name'][name]:
            self.tuya_state['device']['name'][name].append(device_id)

    def _cleanup_device(self, device_id):
        """Cleans up a device's entries."""
        if device_id in self.tuya_state['device']['id']:
            device_info = self.tuya_state['device']['id'].pop(device_id, None)
            if device_info is None:
                self.logger.info(f"Device {device_id} already cleaned up or not found. Ignoring.")
                return
            name = device_info.get('name')
            if name and name in self.tuya_state['device']['name']:
                if device_id in self.tuya_state['device']['name'][name]:
                    self.tuya_state['device']['name'][name].remove(device_id)
                if not self.tuya_state['device']['name'][name]:
                    del self.tuya_state['device']['name'][name]
            try:
                self.task_creator(device_info['device'].close())
            except Exception as e:
                self.logger.warning(f"Failed to close device {device_id} during cleanup: {e}")

    def _get_target_ids(self, payload):
        """Get a list of device IDs from a payload (by 'id' or 'name')."""
        target_ids = []
        if 'id' in payload:
            if payload['id'] in self.tuya_state['device']['id']:
                target_ids.append(payload['id'])
        elif 'name' in payload and payload['name'] in self.tuya_state['device']['name']:
            target_ids.extend(self.tuya_state['device']['name'][payload['name']])
        return target_ids

    def _get_target_devices(self, payload):
        """Get a list of tinytuya device objects from a payload."""
        devices = []
        target_ids = self._get_target_ids(payload)
        for device_id in target_ids:
            if device_id in self.tuya_state['device']['id'] and 'device' in self.tuya_state['device']['id'][device_id]:
                devices.append(self.tuya_state['device']['id'][device_id]['device'])
        return devices

    def _get_device_payloads(self, details=True):
        device_payloads = []
        all_devices_info = self.tuya_state['device']['id'].values()
        for device_info in all_devices_info:
            device_entry = device_info.get('payload', {}).copy()
            if details:
                if 'status' in device_info:
                    device_entry['status'] = device_info['status']
                if 'last_seen' in device_info:
                    device_entry['last_seen'] = device_info['last_seen']
                if 'task' in device_info:
                    children_count = len(device_info.get('children', {}))
                    device_entry['type'] = 'hub device' if children_count > 0 else 'end device'
                    if children_count > 0:
                        device_entry['children_count'] = children_count
                else:
                    device_entry['type'] = 'sub device'
            device_payloads.append(device_entry)
        return device_payloads

    async def _publish_daemon_status(self, payload):
        """Gathers and publishes the daemon's current status."""
        daemon_status = {
            self.root_topic: self.daemon_stat['version'],
            'uptime(min)': int((datetime.now() - self.daemon_stat['start_time']).total_seconds() / 60),
            'mqtt_broker': self.settings['broker']['hostname'],
            'connected_devices_count': len(self.tuya_state['device']['id']),
        }

        if payload['status'] is True:
            daemon_status['devices'] = self._get_device_payloads(True)

        if self.mqtt_publisher:
            await self.mqtt_publisher(
                topic=self.settings['topic']['publish']['daemon'],
                payload=json.dumps(daemon_status, ensure_ascii=False)
            )
        else:
            self.logger.info(json.dumps(daemon_status, ensure_ascii=False))
        self.logger.info('Daemon status requested and published.')

    # --- Tuya Device Listener ---
    async def _process_data(self, device_id, data):
        if 'dps' in data:
            _cid = None
            _name = None
            _device_id = device_id
            if 'data' in data:
                _topic = self.settings['topic']['publish']['active']
                if 'cid' in data['data']:
                    _cid = data['data']['cid']
            else:
                _topic = self.settings['topic']['publish']['passive']
                if 'cid' in data:
                    _cid = data['cid']
            if _cid:
                _device_id = self.tuya_state['device']['id'][device_id]['children'].get(_cid, device_id)
            self.tuya_state['device']['id'][_device_id]['last_seen'] = datetime.now().isoformat()
            _name = self.tuya_state['device']['id'][_device_id].get('name')

            # Customization: Define topic and payload using JSON string templates.
            # The template must be compatible with string substitution ($ notation).
            # Reserved variables for dynamic substitution: $id, $name, $dp, $value, $dps.
            #
            # Example (Single DP): topic='tuya2mqtt/$id/$dp', payload='$value'
            # Example (Multiple DPS): topic='tuya2mqtt/$id', payload='{"data": $dps}'
            _payload_format = self.settings.get('format', {}).get('publish', '')
            if '$value' in _payload_format:
                # Single DataPoint if $value is present.
                for _dp, _value in data['dps'].items():
                    _payload_template = Template(_payload_format)
                    _payload_final = _payload_template.safe_substitute(
                        id=json.dumps(_device_id, ensure_ascii=False),
                        name=json.dumps(_name, ensure_ascii=False),
                        dp=json.dumps(_dp, ensure_ascii=False),
                        value=json.dumps(_value, ensure_ascii=False)
                    )
                    _topic_template = Template(_topic)
                    _topic_final = _topic_template.safe_substitute(
                        id=_device_id, name=_name, dp=_dp
                    )
                    await self.mqtt_publisher(
                        topic=_topic_final,
                        payload=_payload_final
                    )
            else:
                _payload_template = Template(_payload_format)
                _payload_final = _payload_template.safe_substitute(
                    id=json.dumps(_device_id, ensure_ascii=False),
                    name=json.dumps(_name, ensure_ascii=False),
                    dps=json.dumps(data['dps'], ensure_ascii=False)
                )
                _topic_template = Template(_topic)
                _topic_final = _topic_template.safe_substitute(id=_device_id, name=_name)
                await self.mqtt_publisher(topic=_topic_final, payload=_payload_final)

    async def _tuya_receiver(self, payload):
        device_id = payload['id']
        self.logger.info(f"Starting listener for {device_id}")
        reconnect_delay = self.settings['daemon']['retry_delay_seconds']

        try:
            async with DeviceAsync(
                dev_id=payload['id'],
                address=payload.get('ip', 'Auto'),
                local_key=payload.get('key', ''),
                version=payload.get('version', 3.3),
                persist=True
            ) as device:

                self.tuya_state['device']['id'][device_id]['device'] = device

                # Set HB timer
                device.tuya2mqtt_last_sent_time = time.monotonic()

                if 'dev_type' in payload:
                    device.dev_type = payload['dev_type']
                    # Disable autodetect only if dev_type is explicitly set as "default"
                    # Otherwise, keep autodetect enabled and let tinytuya determine
                    device.disabledetect = (payload['dev_type'] == 'default')

                while True:
                    if time.monotonic() - device.tuya2mqtt_last_sent_time >= self.settings['daemon']['heartbeat_interval']:
                        await device.heartbeat()
                        device.tuya2mqtt_last_sent_time = time.monotonic()

                    try:
                        data = await device.receive()
                        if data is not None:
                            if 'Error' in data:
                                for key in data:
                                    if isinstance(data[key], bytes):
                                        data[key] = data[key].decode('utf-8')
                                if 'data unvalid' in data.get('invalid_json', '') and float(device.version) == 3.5:
                                    # 3.5 device does not response to status
                                    data = device.cached_status(True)
                                else:
                                    # Connection error
                                    if device_id in self.tuya_state['device']['id']:
                                        if self.tuya_state['device']['id'][device_id]['status'] != data['Error']:
                                            self.tuya_state['device']['id'][device_id]['status'] = data['Error']
                                    if self.mqtt_publisher:
                                        _publish_id = device_id
                                        _publish_name = self.tuya_state['device']['id'].get(device_id).get('name', '')
                                        _publish_payload = {'id': _publish_id}
                                        if _publish_name: _publish_payload['name'] = _publish_name
                                        _publish_payload.update(data)
                                        _topic_template = Template(self.settings['topic']['publish']['message'])
                                        _topic_final = _topic_template.safe_substitute(id=_publish_id, name=_publish_name)
                                        await self.mqtt_publisher(
                                            topic=_topic_final,
                                            payload=json.dumps(_publish_payload, default=lambda _obj: str(_obj), ensure_ascii=False))
                                    else:
                                        self.logger.error(json.dumps(_publish_payload, default=lambda _obj: str(_obj), ensure_ascii=False))

                                    # TinyTuya does automatically reconnect.
                                    # Devices can sometimes take a while to re-connect to the WiFi, so if you get that error you can just wait a bit and retry the send/receive.
                                    await asyncio.sleep(reconnect_delay)
                                    reconnect_delay = min(reconnect_delay * 2, self.settings['daemon']['max_retry_delay_seconds'])
                                    continue
                            if reconnect_delay != self.settings['daemon']['retry_delay_seconds']:
                                self.logger.info(f"Device {device_id} reconnected successfully. Resetting backoff delay.")
                                reconnect_delay = self.settings['daemon']['retry_delay_seconds']
                            await self._process_data(device_id=device_id, data=data)
                        if device_id in self.tuya_state['device']['id']:
                            if self.tuya_state['device']['id'][device_id]['status'] != 'online':
                                self.tuya_state['device']['id'][device_id]['status'] = 'online'
                                await self._snapshot(True, debounce=5)

                    except Exception as e:
                        self.logger.error(f"Error in receive loop for {device_id}: {e}")
                        self.logger.error(traceback.format_exc())
                        if device_id in self.tuya_state['device']['id']:
                            if self.tuya_state['device']['id'][device_id]['status'] != str(e):
                                self.tuya_state['device']['id'][device_id]['status'] = str(e)
                                await self._snapshot(True, debounce=5)
                        await asyncio.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, self.settings['daemon']['max_retry_delay_seconds'])

        except asyncio.CancelledError:
            self.logger.info(f"Listener task for {device_id} was cancelled.")
        except Exception:
            self.logger.error(f"Unhandled exception: {traceback.format_exc()}")
        finally:
            if device_id in self.tuya_state['device']['id']:
                for _, child_id in self.tuya_state['device']['id'][device_id].get('children', {}).items():
                    self._cleanup_device(child_id)
                self._cleanup_device(device_id)
            self.logger.info(f"Cleaned up resources for {device_id}.")

    # --- Mqtt Lock ---
    async def _mqtt_lock(self, mqtt):
        await mqtt.subscribe(self.settings['topic']['publish']['daemon'])
        await mqtt.publish(topic=self.settings['topic']['subscribe']['query'], payload='{"status":false}', retain=True)
        try:
            await asyncio.wait_for(mqtt.messages.__anext__(), timeout=self.settings['daemon']['instance_lock_timeout'])
            return True
        except:
            return False
        finally:
            await mqtt.publish(topic=self.settings['topic']['subscribe']['query'], retain=True)
            await mqtt.unsubscribe(self.settings['topic']['publish']['daemon'])

    # --- Mqtt Handler ---
    async def _mqtt_handler(self):
        reconnect_delay = self.settings['daemon']['retry_delay_seconds']
        while True:
            try:
                # 1. Cooperative Check (via anonymous client):
                #    A graceful pre-check using a retained message.
                async with aiomqtt.Client(
                    hostname=self.settings['broker']['hostname'],
                    port=self.settings['broker']['port'],
                    username=self.settings['broker'].get('username'),
                    password=self.settings['broker'].get('password'),
                ) as client:
                    self.logger.info('Successfully connected to MQTT broker.')

                    if await self._mqtt_lock(client):
                        self.logger.info(f"Another instance is already running.")
                        self.shutdown_event.set()
                        break

                # 2. Enforced Lock (via unique client ID):
                #    A fail-safe against race conditions.
                async with aiomqtt.Client(
                    hostname=self.settings['broker']['hostname'],
                    port=self.settings['broker']['port'],
                    username=self.settings['broker'].get('username'),
                    password=self.settings['broker'].get('password'),
                    identifier=f"{self.root_topic}_{self.daemon_stat['version']}",
                ) as self.mqtt:

                    if reconnect_delay != self.settings['daemon']['retry_delay_seconds']:
                        self.logger.info(f"Mqtt reconnected successfully. Resetting backoff delay.")
                        reconnect_delay = self.settings['daemon']['retry_delay_seconds']
                    self.mqtt_publisher = self.mqtt.publish

                    # Subscribe to all necessary topics
                    for topic in self.settings['topic']['subscribe'].values():
                        subscribe_topic = re.sub(r"\$\w+", "+", topic)
                        await self.mqtt.subscribe(subscribe_topic)
                        self.logger.info(f"Topic subscribed: {topic}")
                    await self.mqtt.subscribe(self.settings['topic']['publish']['snapshot'])
                    await self.mqtt.subscribe(self.root_topic)
                    await self.mqtt_publisher(topic=self.root_topic, payload=json.dumps(self.settings['topic']), retain=True)

                    # Main message processing loop
                    async for message in self.mqtt.messages:
                        self.task_creator(self.recv_topic(message))

            except aiomqtt.MqttError as error:
                try:
                    if 'code:128' in str(error):
                        # Duplicated identifier
                        print(error)
                        self.shutdown_event.set()
                        break
                except Exception:
                    pass
                print(f"MQTT connection error: {error}. Reconnecting...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, self.settings['daemon']['max_retry_delay_seconds'])
            except asyncio.CancelledError:
                break
            except Exception:
                self.logger.error(f"An unexpected error occurred: {traceback.format_exc()}")
                await self.terminator(True)
        self.logger.info('MQTT disconnected')

    # --- Main Application ---
    # =========================================================================
    # Execution Modes and Event Loop Management
    # =========================================================================

    # 1. Self-contained Execution (Internal Loop):
    #    Call start(). This method executes asyncio.run() internally to begin
    #    the loop and manage all tasks. (Blocks until completion)

    # 2. External Event Loop Integration (Task Creation):
    #    Call run(). This method returns a main async task object ready to be
    #    integrated into an existing external event loop (e.g., via create_task).

    # -------------------------------------------------------------------------

    # 3. Using an External MQTT Client/Publisher (Advanced):
    #    If integrating with an external MQTT client, follow these steps:
    #  A. DO NOT execute the default start() or run() methods.
    #  B. After initialization, you MUST set the publisher:
    #     tuya2mqtt.mqtt_publisher = your_external_client.publish
    #     (Or pass it during __init__(..., mqtt_publisher=your_external_client.publish))
    #  C. The external MQTT client must handle topic subscription internally.
    #  D. Upon receiving a message, dispatch the handling task by calling:
    #     task_creator(tuya2mqtt.recv_topic(message)), which returns after each message processing is done.
    #  E. Publish initial topic informations to 'tuya2mqtt' with the retain flag enabled upon launch.
    async def run(self, signal_handler=True):
        if signal_handler:
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGTERM, lambda: self.task_creator(self.terminator(True)))
        self.mqtt_task = self.task_creator(self._mqtt_handler())
        try:
            await self.shutdown_event.wait()
        except asyncio.CancelledError:
            print("Shutting down.")
            try:
                await self.terminator(True)
            except Exception as e:
                print(f"Error: {e}")
        finally:
            if not self.mqtt_task.done():
                self.mqtt_task.cancel()
                try:
                    await self.mqtt_task
                except asyncio.CancelledError:
                    pass
            print("Daemon terminated.")

    def start(self):
        """Wrapper to run the main async function."""
        # Launch main loop
        try:
            asyncio.run(self.run())
        except KeyboardInterrupt:
            print("Foreground process interrupted by user.")

    @staticmethod
    def config_parser(parser, filename, arg_list):
        def get_env_default(env_var_name, default=None, type_func=str):
            value = os.getenv(env_var_name)
            if value is not None:
                try:
                    return type_func(value)
                except ValueError:
                    print(f"Warning: Environment variable {env_var_name} has an invalid type and will be ignored.")
            return default
        def update_nested_dict(current_dict, keys, final_value, overwrite=True):
            if not keys: return
            current_key = keys[0]
            if len(keys) == 1:
                if current_dict.get(current_key) is None or overwrite:
                    current_dict[current_key] = final_value
                return
            if current_key not in current_dict or not isinstance(current_dict[current_key], dict):
                current_dict[current_key] = {}
            update_nested_dict(current_dict[current_key], keys[1:], final_value, overwrite)
        parser.add_argument('--config', type=str, default=filename, dest='config')
        for arg_single in arg_list:
            parser.add_argument(
                f"--{'-'.join(arg_single['name']).lower()}",
                default=argparse.SUPPRESS,
                type=arg_single['type'],
                dest='_'.join(arg_single['name']).lower()
            )
        args = parser.parse_args()
        config = {}
        try:
            with open(args.config, 'r', encoding='utf-8') as file:
                config = json.load(file)
        except FileNotFoundError:
            # Using env/cli settings.
            pass
        except Exception as e:
            print(f"An error occurred while reading config file: {e}. Using env/cli settings.")
        for arg_single in arg_list:
            env_val = get_env_default(
                '_'.join(arg_single['name']).upper(),
                default=None,
                type_func=arg_single['type']
            )
            if env_val is not None:
                update_nested_dict(config, arg_single['name'], env_val)
        for arg_single in arg_list:
            cli_key = '_'.join(arg_single['name']).lower()
            if hasattr(args, cli_key):
                update_nested_dict(config, arg_single['name'], getattr(args, cli_key))
            else:
                update_nested_dict(config, arg_single['name'], arg_single['default'], False)
        return args, config


# --- main() ---
def main():
    # Initialize paths
    WORK_FOLDER = os.path.dirname(os.path.realpath(__file__))
    FILE_NAME = os.path.splitext(os.path.basename(__file__))[0]
    PID_FILE = f"{WORK_FOLDER}/{FILE_NAME}.pid"
    DEFAULT_CONF_FILE = f"{WORK_FOLDER}/{FILE_NAME}.conf"

    # Command-line argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', type=str, dest='mode', default='start', choices=['start', 'stop', 'restart', 'foreground'])
    add_argument_list = [
        {'name': ['broker', 'hostname'], 'default': 'localhost', 'type': str},
        {'name': ['broker', 'port'], 'default': 1883, 'type': int},
        {'name': ['broker', 'username'], 'default': '', 'type': str},
        {'name': ['broker', 'password'], 'default': '', 'type': str},
    ]
    args, config = Tuya2MQTTBridge.config_parser(parser, DEFAULT_CONF_FILE, add_argument_list)

    no_daemon = False
    try:
        import daemon
        from daemon import pidfile
    except ImportError:
        print("Cannot import daemon module.")
        no_daemon = True
    if args.mode == 'foreground' or no_daemon:
        print("Running in foreground mode...")
        bridge = Tuya2MQTTBridge(foreground=True, **config)
        bridge.start()
        no_daemon = True
    if no_daemon:
        sys.exit(0)

    # --- Daemon control logic ---
    pid_file_obj = pidfile.TimeoutPIDLockFile(PID_FILE)
    if args.mode in {'stop', 'restart'}:
        print("Stopping daemon...")
        try:
            if pid_file_obj.is_locked():
                pid = pid_file_obj.read_pid()
                os.kill(pid, signal.SIGTERM)
                terminated_gracefully = False
                for _ in range(10):
                    time.sleep(0.5)
                    try:
                        os.kill(pid, 0)
                    except ProcessLookupError:
                        print("Process stopped successfully.")
                        terminated_gracefully = True
                        break

                if not terminated_gracefully:
                    print(f"Process {pid} did not terminate gracefully. Sending SIGKILL.")
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        print("Process was already gone before SIGKILL.")
            else:
                print("PID file not locked. Is the daemon running?")
        except Exception as e:
            print(f"Error while stopping: {e}")
        finally:
            if os.path.exists(PID_FILE):
                os.remove(PID_FILE)
        if args.mode == 'stop':
            sys.exit(0)
    if args.mode in {'start', 'restart'}:
        if pid_file_obj.is_locked():
            print(f"Daemon already running with PID {pid_file_obj.read_pid()}. Use 'restart' or 'stop' first.")
            sys.exit(1)

        print("Starting daemon...")
        context = daemon.DaemonContext(
            working_directory=WORK_FOLDER,
            umask=0o002,
            pidfile=pid_file_obj
        )
        with context:
            bridge = Tuya2MQTTBridge(**config)
            bridge.start()


# --- Entry Point ---
if __name__ == "__main__":
    main()