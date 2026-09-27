"""`render_command` from Python: the command a client publishes, read back by the bridge's own parser
(`match_topic` + `parse_payload`) as exactly the request it was made from."""

import json

import pytest

import pyrustuyabridge as pb

PER_DP = "rustuya/command/{action}/{id}/{dp}"


def read_back(template, topic, payload):
    parsed = pb.parse_payload(payload, pb.match_topic(topic, template))
    parsed.pop("dp", None)
    return {k: v for k, v in parsed.items() if v is not None}


@pytest.mark.parametrize("value", [True, False, 50, 2.5, "white"])
def test_one_dp_set_is_the_bare_value_on_the_dp_topic(value):
    request = {"action": "set", "id": "eb1", "dps": {"20": value}}
    topic, payload = pb.render_command(PER_DP, request)
    assert topic == "rustuya/command/set/eb1/20"
    assert json.loads(payload) == value
    assert read_back(PER_DP, topic, payload) == request


@pytest.mark.parametrize("request_", [
    {"action": "set", "id": "eb1", "dps": {"21": "colour", "24": "000003e803e8"}},
    {"action": "status", "id": "bridge", "offset": 50},
    {"action": "get", "id": "eb1"},
])
def test_other_requests_are_one_request_object(request_):
    topic, payload = pb.render_command(PER_DP, request_)
    assert "{" not in topic
    assert json.loads(payload) == request_
    assert read_back(PER_DP, topic, payload) == request_


def test_a_template_without_placeholders():
    request = {"action": "set", "id": "eb1", "dps": {"1": True}}
    topic, payload = pb.render_command("rustuya/command", request)
    assert topic == "rustuya/command" and json.loads(payload) == request


def test_what_cannot_be_expressed_is_none():
    assert pb.render_command(PER_DP, {"id": "eb1"}) is None
    assert pb.render_command(PER_DP, {"action": "get", "id": ["a", "b"]}) is None
