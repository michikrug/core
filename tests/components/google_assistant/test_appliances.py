"""Test explicit read-only appliance types."""

from unittest.mock import patch

import probatio
import pytest

from homeassistant.components.google_assistant import GOOGLE_ASSISTANT_SCHEMA
from homeassistant.components.google_assistant.helpers import async_get_entities
from homeassistant.components.google_assistant.http import GoogleConfig
from homeassistant.components.google_assistant.smart_home import async_handle_message
from homeassistant.core import HomeAssistant

from . import mock_google_config_store
from .test_http import DUMMY_CONFIG

ENTITY_ID = "binary_sensor.appliance"


@pytest.fixture(params=["washer", "dryer", "dishwasher"])
def appliance_config(
    request: pytest.FixtureRequest, hass: HomeAssistant
) -> GoogleConfig:
    """Configure an explicitly typed appliance."""
    config = GoogleConfig(
        hass,
        GOOGLE_ASSISTANT_SCHEMA(
            {
                **DUMMY_CONFIG,
                "entity_config": {ENTITY_ID: {"type": request.param}},
            }
        ),
    )
    config._store = mock_google_config_store()
    return config


async def test_sync(hass: HomeAssistant, appliance_config: GoogleConfig) -> None:
    """Only explicitly typed running sensors are exposed as appliances."""
    hass.states.async_set(ENTITY_ID, "on", {"device_class": "running"})
    hass.states.async_set("binary_sensor.untyped", "on", {"device_class": "running"})
    result = await async_handle_message(
        hass,
        appliance_config,
        "user",
        "user",
        {"requestId": "test", "inputs": [{"intent": "action.devices.SYNC"}]},
        "cloud",
    )
    assert result is not None
    devices = result["payload"]["devices"]
    assert len(devices) == 1
    assert devices[0]["id"] == ENTITY_ID
    assert devices[0]["type"] == (
        "action.devices.types."
        + appliance_config.entity_config[ENTITY_ID]["type"].upper()
    )
    assert devices[0]["traits"] == ["action.devices.traits.StartStop"]
    assert devices[0]["attributes"] == {"pausable": False}


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        pytest.param("on", {"online": True, "isRunning": True}, id="running"),
        pytest.param("off", {"online": True, "isRunning": False}, id="stopped"),
        pytest.param("unknown", {"online": False}, id="unknown"),
        pytest.param("unavailable", {"online": False}, id="unavailable"),
    ],
)
async def test_query(
    hass: HomeAssistant,
    appliance_config: GoogleConfig,
    state: str,
    expected: dict[str, bool],
) -> None:
    """Return running status without treating unknown as stopped."""
    hass.states.async_set(ENTITY_ID, state, {"device_class": "running"})
    result = await async_handle_message(
        hass,
        appliance_config,
        "user",
        "user",
        {
            "requestId": "test",
            "inputs": [
                {
                    "intent": "action.devices.QUERY",
                    "payload": {"devices": [{"id": ENTITY_ID}]},
                }
            ],
        },
        "cloud",
    )
    assert result is not None
    assert result["payload"] == {"devices": {ENTITY_ID: expected}}


@pytest.mark.parametrize(
    ("command", "params"),
    [
        pytest.param("StartStop", {"start": True}, id="start"),
        pytest.param("StartStop", {"start": False}, id="stop"),
        pytest.param("PauseUnpause", {"pause": True}, id="pause"),
        pytest.param("PauseUnpause", {"pause": False}, id="unpause"),
    ],
)
async def test_execute(
    hass: HomeAssistant,
    appliance_config: GoogleConfig,
    command: str,
    params: dict[str, bool],
) -> None:
    """Reject controls for read-only appliance sensors."""
    hass.states.async_set(ENTITY_ID, "on", {"device_class": "running"})
    result = await async_handle_message(
        hass,
        appliance_config,
        "user",
        "user",
        {
            "requestId": "test",
            "inputs": [
                {
                    "intent": "action.devices.EXECUTE",
                    "payload": {
                        "commands": [
                            {
                                "devices": [{"id": ENTITY_ID}],
                                "execution": [
                                    {
                                        "command": f"action.devices.commands.{command}",
                                        "params": params,
                                    }
                                ],
                            }
                        ]
                    },
                }
            ],
        },
        "cloud",
    )
    assert result is not None
    assert result["payload"] == {
        "commands": [
            {"ids": [ENTITY_ID], "status": "ERROR", "errorCode": "functionNotSupported"}
        ]
    }


@pytest.mark.parametrize(
    ("entity_id", "device_class"),
    [
        pytest.param("switch.appliance", None, id="wrong-domain"),
        pytest.param(ENTITY_ID, "motion", id="wrong-device-class"),
    ],
)
async def test_incompatible_entity(
    hass: HomeAssistant,
    entity_id: str,
    device_class: str | None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Reject appliance types on entities without running status."""
    config = GoogleConfig(
        hass,
        GOOGLE_ASSISTANT_SCHEMA(
            {
                **DUMMY_CONFIG,
                "entity_config": {entity_id: {"type": "washer"}},
            }
        ),
    )
    hass.states.async_set(entity_id, "on", {"device_class": device_class})
    assert async_get_entities(hass, config) == []
    assert "Appliance type requires a running binary sensor" in caplog.text


def test_invalid_type() -> None:
    """Reject unknown appliance types in YAML."""
    with pytest.raises(probatio.Invalid):
        GOOGLE_ASSISTANT_SCHEMA(
            {**DUMMY_CONFIG, "entity_config": {ENTITY_ID: {"type": "invalid"}}}
        )


async def test_reload(hass: HomeAssistant, appliance_config: GoogleConfig) -> None:
    """Refresh support when appliance configuration is reloaded."""
    hass.states.async_set(ENTITY_ID, "on", {"device_class": "running"})
    assert len(async_get_entities(hass, appliance_config)) == 1
    with patch(
        "homeassistant.components.google_assistant.http.async_integration_yaml_config",
        return_value={"google_assistant": DUMMY_CONFIG},
    ):
        assert await appliance_config.async_reload_yaml()
    assert async_get_entities(hass, appliance_config) == []
    with patch(
        "homeassistant.components.google_assistant.http.async_integration_yaml_config",
        return_value={
            "google_assistant": {
                **DUMMY_CONFIG,
                "entity_config": {ENTITY_ID: {"type": "washer"}},
            }
        },
    ):
        assert await appliance_config.async_reload_yaml()
    assert len(async_get_entities(hass, appliance_config)) == 1
