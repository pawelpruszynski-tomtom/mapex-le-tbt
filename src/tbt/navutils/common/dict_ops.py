"""Utility dict functions and types"""
from typing import Dict, Union, TypeVar, Any, Hashable, Callable

NestedDict = Dict[str, Union[float, "NestedDict"]]
CallableDict = Dict[str, Callable[..., Union[int, float]]]
NestedCallableDict = Dict[
    str, Union[Dict[str, Callable[..., Union[int, float]]], "NestedCallableDict"]
]
AnyDict = TypeVar("AnyDict", bound=Dict[Any, Any])


def dict_update(original_dict: AnyDict, updated_values: AnyDict) -> AnyDict:
    """Update a multilevel dict

    :param original_dict: dict to update
    :type original_dict: AnyDict
    :param updated_values: values updated
    :type updated_values: AnyDict
    :return: `original_dict` with the `updated_values`
    :rtype: AnyDict
    """
    for key, value in updated_values.items():
        if isinstance(value, dict):
            original_dict[key] = dict_update(original_dict.get(key, {}), value)
        else:
            original_dict[key] = value
    return original_dict


def process_dict(
    dictionary: Union[CallableDict, NestedCallableDict], **kwargs
) -> NestedDict:
    """Method to call all callables on a dict and save the return value


    :param dictionary: callables dictionary
    :type dictionary: NestedCallableDict
    :return: dict with the same keys as `dictinary` and the results of all callables
    :rtype: NestedDict
    """
    result: NestedDict = {}
    for key, value in dictionary.items():
        if isinstance(value, dict):
            result[key] = process_dict(value, **kwargs)  # type: ignore
        else:
            result[key] = value(**kwargs)
    return result


def flatten_dict(
    dictionary: Dict[Any, Any], separator: str = ".", prefix: str = ""
) -> Dict[Any, Any]:
    """Flats an anidated dict into one-level only dict

    :param dictionary: dictionary to flatten
    :type dictionary: Dict[Any, Any]
    :param separator: charecter used to flat levels, defaults to "."
    :type separator: str, optional
    :param prefix: key of the first level if needed, defaults to ""
    :type prefix: str, optional
    :return: One level deepth dict with keys collapsed using `separator`
    :rtype: Dict[Any, Any]
    """
    return (
        {
            prefix + separator + k if prefix else k: v
            for kk, vv in dictionary.items()
            for k, v in flatten_dict(vv, separator, kk).items()
        }
        if isinstance(dictionary, dict)
        else {prefix: dictionary}
    )


def unflatten_dict(dictionary: Dict[Any, Any], separator: str = ".") -> Dict[Any, Any]:
    """Exapnad a flattenned dict

    :param dictionary: Dictionary to unflatten
    :type dictionary: Dict[Any, Any]
    :param separator: separator used to collapse the dict, defaults to "."
    :type separator: str, optional
    :return: A multilevel depth dict
    :rtype: Dict[Any, Any]
    """
    result: Dict[Any, Any] = {}
    for key, value in dictionary.items():
        parts = key.split(separator)
        current = result
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value
    return result


GenericHashable = TypeVar("GenericHashable", bound=Hashable)


def rename_key(
    dictionary: Dict[GenericHashable, Any],
    old_key: GenericHashable,
    new_key: GenericHashable,
):
    """Rename any appearance of `old_key` with `new_key` on `dictionary`

    :param dictionary: Any python dict
    :type dictionary: Dict[GenericHashable, Any]
    :param old_key: key replaced
    :type old_key: GenericHashable
    :param new_key: key to be replaced with
    :type new_key: GenericHashable
    """
    for key in list(dictionary.keys()):
        if key == old_key:
            dictionary[new_key] = dictionary.pop(old_key)
        elif isinstance(dictionary[key], dict):
            rename_key(dictionary[key], old_key, new_key)


def append_dictionary(dictionary: Dict[str, Any], suffix: str, separator: str = "."):
    """Append `suffix` to all keys to a flattened dict using `separator`

    :param dictionary: dict with str keys, must have been flattened with `separator`
    :type dictionary: Dict[str, Any]
    :param suffix: suffix to append
    :type suffix: str
    :param separator: separator used on join method, defaults to "."
    :type separator: str, optional
    """
    for key in list(dictionary.keys()):
        names = key.split(separator)
        names[-1] = f"{names[-1]}_{suffix}"
        dictionary[separator.join(names)] = dictionary.pop(key)
