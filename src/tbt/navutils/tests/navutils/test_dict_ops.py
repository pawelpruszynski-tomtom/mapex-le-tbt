""" Test for dict_ops """
import math
from ...common.dict_ops import (
    dict_update,
    process_dict,
    flatten_dict,
    unflatten_dict,
    rename_key,
    append_dictionary,
)


def _compare_dicts(dict1, dict2, key=""):
    if len(dict1) != len(dict2):
        return False, key if key else "<first_level>", "<different size>"
    for k in dict1:
        if k not in dict2:
            return False, key + " -> " + k if key else k, dict1[k]
        if not isinstance(dict1[k], type(dict2[k])):
            return False, key + " -> " + k if key else k, dict1[k]
        if isinstance(dict1[k], dict):
            result, key, value = _compare_dicts(dict1[k], dict2[k], key=k)
            if not result:
                return False, key, value
        elif isinstance(dict1[k], int):
            if dict1[k] != dict2[k]:
                return False, key + " -> " + k if key else k, dict1[k]
        elif isinstance(dict1[k], float):
            if not math.isclose(dict1[k], dict2[k]):
                return False, key + " -> " + k if key else k, dict1[k]
    return True, None, None


def test_dict_update():
    """Test for dict_update just anidated dict"""
    original_dict = {
        "key_0": 0,
        "key_1": 1,
        "key_sub_0": {"sub_0_key_0": 0, "sub_0_key_1": 1},
        "key_sub_1": {"sub_1_key_0": 0.0, "sub_1_key_1": 1.0},
    }

    updated_values = {
        "u_key_0": 2,
        "key_sub_0": {"sub_0_u_key_0": 2},
        "key_sub_1": {"sub_1_u_key_0": 2.0},
    }

    updated_dict = dict_update(
        original_dict=original_dict, updated_values=updated_values
    )

    expected_result = {
        "key_0": 0,
        "key_1": 1,
        "key_sub_0": {"sub_0_key_0": 0, "sub_0_key_1": 1, "sub_0_u_key_0": 2},
        "key_sub_1": {"sub_1_key_0": 0.0, "sub_1_key_1": 1.0, "sub_1_u_key_0": 2.0},
        "u_key_0": 2,
    }
    test_compare = _compare_dicts(updated_dict, expected_result)

    assert test_compare[
        0
    ], f"Distinct value at key={test_compare[1]} with value={test_compare[2]}"


def test_process_dict():
    """Test for process_dict"""
    callable_dict = {
        "test_0": lambda arg: 0,
        "test_1": lambda arg: 1.0,
        "test_2": {"test_3": lambda arg: 3},
    }

    result_dict = process_dict(dictionary=callable_dict, arg="")

    expected_result = {"test_0": 0, "test_1": 1.0, "test_2": {"test_3": 3}}
    test_compare = _compare_dicts(result_dict, expected_result)

    assert test_compare[
        0
    ], f"Distinct value at key={test_compare[1]} with value={test_compare[2]}"


def test_flatten_dict():
    """Test flatten_dict"""
    original_dict = {
        "a": 0,
        "b": 1,
        "c": {"a": 0, "b": 1},
        "d": {"a": 0.0, "b": 1.0},
    }

    flattened_dict = flatten_dict(dictionary=original_dict, separator="_")

    expected_result = {
        "a": 0,
        "b": 1,
        "c_a": 0,
        "c_b": 1,
        "d_a": 0.0,
        "d_b": 1.0,
    }

    test_compare = _compare_dicts(flattened_dict, expected_result)

    assert test_compare[
        0
    ], f"Distinct value at key={test_compare[1]} with value={test_compare[2]}"


def test_unflatten_dict():
    """Test unflatten_dict"""
    original_dict = {
        "a": 0,
        "b": 1,
        "c_a": 0,
        "c_b": 1,
        "d_a": 0.0,
        "d_b": 1.0,
    }

    flattened_dict = unflatten_dict(dictionary=original_dict, separator="_")

    expected_result = {
        "a": 0,
        "b": 1,
        "c": {"a": 0, "b": 1},
        "d": {"a": 0.0, "b": 1.0},
    }

    test_compare = _compare_dicts(flattened_dict, expected_result)

    assert test_compare[
        0
    ], f"Distinct value at key={test_compare[1]} with value={test_compare[2]}"


def test_rename_key():
    """Test rename_key"""
    to_rename_dict = {"a": 0, "b": 1, "c": {"b": 2}}

    rename_key(dictionary=to_rename_dict, old_key="b", new_key="x")

    expected_result = {"a": 0, "x": 1, "c": {"x": 2}}

    test_compare = _compare_dicts(to_rename_dict, expected_result)

    assert test_compare[
        0
    ], f"Distinct value at key={test_compare[1]} with value={test_compare[2]}"


def test_append_dictionary():
    """Test append_dictionary"""
    to_append_dict = {
        "a": 0,
        "b": 1,
        "c.a": 0,
        "c.b": 1,
        "d.a": 0.0,
        "d.b": 1.0,
    }

    append_dictionary(dictionary=to_append_dict, suffix="x", separator=".")

    expected_result = {
        "a_x": 0,
        "b_x": 1,
        "c.a_x": 0,
        "c.b_x": 1,
        "d.a_x": 0.0,
        "d.b_x": 1.0,
    }

    test_compare = _compare_dicts(to_append_dict, expected_result)

    assert test_compare[
        0
    ], f"Distinct value at key={test_compare[1]} with value={test_compare[2]}"
