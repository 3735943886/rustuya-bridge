"""The constructor's keyword arguments: an unknown one is refused, not ignored.

Ignored, a misspelled option looks accepted and takes no effect: `mqtt_username`
for `mqtt_user` connected to the broker with no credentials at all.
"""

import pytest

pyrustuyabridge = pytest.importorskip("pyrustuyabridge")
PyBridgeServer = pyrustuyabridge.PyBridgeServer


@pytest.mark.parametrize("name", ["mqtt_username", "mqtt_pass", "not_an_option"])
def test_an_unknown_keyword_is_refused_by_name(name):
    with pytest.raises(TypeError, match=name):
        PyBridgeServer(**{name: "value"})


def test_an_unknown_keyword_is_refused_beside_known_ones():
    with pytest.raises(TypeError, match="mqtt_username"):
        PyBridgeServer(mqtt_broker="mqtt://127.0.0.1:1", mqtt_username="user")


def test_known_keywords_are_taken():
    server = PyBridgeServer(mqtt_user="user", mqtt_password="secret", log_level="warn")
    assert server.config_snapshot()["log_level"] == "warn"


def test_no_keywords_at_all():
    PyBridgeServer()
