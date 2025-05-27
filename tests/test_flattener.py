import pytest
from flattener import Flattener  # adjust class name if it's different

def test_flatten_basic_policy():
    flattener = Flattener()
    input_data = {
        "policy": {
            "policy_id": {"tag": "string", "value": "P123"},
            "status": {"tag": "string", "value": "Active"}
        }
    }

    expected = {
        "policy.policy_id": "P123",
        "policy.status": "Active"
    }

    result = flattener.flatten(input_data)
    assert result == expected

def test_flatten_with_missing_value():
    flattener = Flattener()
    input_data = {
        "policy": {
            "policy_id": {"tag": "string"},  # no "value" key
            "status": {"tag": "string", "value": "Cancelled"}
        }
    }

    expected = {
        "policy.status": "Cancelled"
    }

    result = flattener.flatten(input_data)
    assert result == expected

def test_flatten_with_array_of_structs():
    flattener = Flattener()
    input_data = {
        "policy": {
            "holders": [
                {"name": {"tag": "string", "value": "Alice"}},
                {"name": {"tag": "string", "value": "Bob"}}
            ]
        }
    }

    expected = {
        "policy.holders[0].name": "Alice",
        "policy.holders[1].name": "Bob"
    }

    result = flattener.flatten(input_data)
    assert result == expected
