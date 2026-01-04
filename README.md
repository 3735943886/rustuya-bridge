# Rustuya Bridge

A ZMQ-based bridge server for managing Tuya devices via `rustuya`.

## How to Run

### Direct Execution
```bash
cargo run --release -- --command-addr tcp://0.0.0.0:37358 --event-addr tcp://0.0.0.0:37359
```

### Docker Execution
Run with **host network mode** to ensure Tuya device discovery works correctly:
```bash
docker run -d --name rustuya --network host -v $(pwd)/data:/data 3735943886/rustuya
```

## Python Example

### Sending Actions
```python
import zmq

# Command Socket
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://127.0.0.1:37358")
socket.setsockopt(zmq.RCVTIMEO, 2000) # 2s timeout

def send_command(payload):
    socket.send_json(payload)
    return socket.recv_json()

print(send_command({"action": "manager/add", "id": "DEVICE_ID", "key": "DEVICE_KEY"}))
print(send_command({"action": "manager/status"}))
print(send_command({"action": "device/status", "id": "DEVICE_ID"}))
```

### Listening Events
```python
import zmq

# Event Socket
context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://127.0.0.1:37359")
socket.subscribe("") # Subscribe to all devices

while True:
    topic, payload = socket.recv_multipart()
    print(f"[{topic.decode()}] {payload.decode()}")
```

### Asynchronous
```python
import asyncio
import zmq.asyncio

async def main():
    ctx = zmq.asyncio.Context()
    
    # 1. Command Socket (DEALER)
    cmd_socket = ctx.socket(zmq.DEALER)
    cmd_socket.connect("tcp://127.0.0.1:37358")
    
    # 2. Event Socket (SUB)
    sub_socket = ctx.socket(zmq.SUB)
    sub_socket.connect("tcp://127.0.0.1:37359")
    sub_socket.subscribe("")

    async def listen_events():
        while True:
            topic, payload = await sub_socket.recv_multipart()
            print(f"[Event] {topic.decode()} -> {payload.decode()}")

    async def listen_commands():
        while True:
            res = await cmd_socket.recv_json()
            print(f"[Response] {res}")

    # Run listeners in background
    asyncio.create_task(listen_events())
    asyncio.create_task(listen_commands())

    # Send actions without blocking
    await cmd_socket.send_json({"action": "manager/add", "id": "DEVICE_ID", "key": "DEVICE_KEY"})
    await cmd_socket.send_json({"action": "manager/status"})
    await cmd_socket.send_json({"action": "device/status", "id": "DEVICE_ID"})
    await cmd_socket.send_json({"action": "device/set_dps", "id": "DEVICE_ID", "dps": {"1": True}})

    # Wait for events/responses
    await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
```

## API Summary
- `manager/status`: List devices and connection status.
- `manager/add`: Add or modify a device configuration.
- `manager/remove`: Remove a device.
- `device/status`: Query DP status.
- `device/set_dps`: Set DP values (requires `dps` object).
- `device/request`: Send raw command (requires `cmd`, `data`).

> **Note**: Add `"cid": "SUB_DEVICE_ID"` to any `device/` action for sub-device control.
