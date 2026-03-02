import os
import yaml


def get_credentials() -> dict:
    """Get credentials yaml file
    :return: Dict containing the info from credentials file
    :rtype: dict
    """
    # root_folder = os.path.dirname(
    #         os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
    #     )

    # creds_file = os.path.join(root_folder, "conf/local/credentials.yml")
    creds_file = "/workspace/conf/local/credentials.yml"
    with open(creds_file) as cred:
        cred_dict = yaml.safe_load(cred)
    return cred_dict


def get_globals() -> dict:
    """Get globals yaml file
    :return: Dict containing the info from credentials file
    :rtype: dict
    """
    # root_folder = os.path.dirname(
    #         os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
    #     )

    # creds_file = os.path.join(root_folder, "conf/local/globals.yml")
    globals_file = "/workspace/conf/local/globals.yml"
    with open(globals_file) as cred:
        cred_dict = yaml.safe_load(cred)
    return cred_dict
