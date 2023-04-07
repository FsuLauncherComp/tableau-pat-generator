import uuid
import json
import requests
import yaml
import tableauserverclient as TSC
import logging

# create a logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create a file handler
file_handler = logging.FileHandler('logs.txt')
file_handler.setLevel(logging.DEBUG)

# create a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# add the file handler to the logger
logger.addHandler(file_handler)

def viz_portal_call(server, payload: dict) -> dict:
    """Makes a call to the vizportal API"""
    logging.debug(f"Sending request to {server.server_address}")
    endpoint = payload.get("method")
    url = f"{server.server_address}/vizportal/api/web/v1/{endpoint}"
    json_payload = json.dumps(payload)
    headers = {
        "cache-control": "no-cache",
        "accept": "application/json, text/plain, */*",
        "x-xsrf-token": "",
        "content-type": "application/json;charset=UTF-8",
        "cookie": f"workgroup_session_id={server.auth_token}; XSRF-TOKEN=",
    }

    response = requests.request(
        "POST", url, headers=headers, data=json_payload, verify=False
    )
    logging.debug(f"Response status code: {response.status_code}")
    if response.status_code != 200:
        logging.error(f"Response: {response.text}")
        raise Exception(f"Response: {response.text}")

    return response.json()


def create_pat_payload() -> dict:
    """Creates the payload for creating a PAT token"""
    pat_token_name = str(uuid.uuid4())
    payload = {
        "method": "createPersonalAccessToken",
        "params": {"clientId": pat_token_name},
    }
    logging.debug(f"PAT Payload: {payload}")
    return payload

def find_user_by_name(server: TSC.Server, user_name: str) -> str:
    """Finds the user id by name. Usernames are unique in Tableau"""
    request_options = TSC.RequestOptions()
    request_options.filter.add(
        TSC.Filter(
            TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, user_name
        )
    )
    logging.debug(f"Request options: {request_options}")
    all_users, pagination_item = server.users.get(request_options)
    user_id = all_users[0].id
    logging.debug(f"Found user with id {user_id}")
    return user_id


def create_pat_token(server: TSC.Server) -> str:
    """Creates a PAT token for the user"""
    logging.debug("Creating PAT token")
    pat_payload: dict[str, str] = create_pat_payload()
    pat_response: dict[str, dict[str, str]] = viz_portal_call(server, pat_payload)
    result: str = pat_response["result"]
    return result

def create_text_dump(user_id: str, token_name: str, token_value: str) -> str:
    """Creates a text dump of the user id, token name, and token value"""
    logging.debug("Creating text dump")
    return {
        "user_id": user_id, "token_name": token_name, "token_value": token_value
    }


def create_tableau_auth_as_users(tableau_configs: dict, user_id: str =None) -> TSC.TableauAuth:
    """Creates a Tableau Auth object as the user"""
    logging.debug("Creating Tableau Auth as user with id {user_id}")
    server_auth = TSC.TableauAuth(
        tableau_configs["username"],
        tableau_configs["password"],
        site_id=tableau_configs["site_name"],
        user_id_to_impersonate=user_id,
    )
    return server_auth


def admin_auth(tableau_configs: dict) -> TSC.TableauAuth:
    """Creates a Tableau Auth object as the admin"""
    logging.debug("Creating Tableau Auth as admin")
    server_auth = TSC.TableauAuth(
        tableau_configs["username"],
        tableau_configs["password"],
        site_id=tableau_configs["site_name"],
    )
    return server_auth

if __name__ == "__main__":
    with open("configs.yml", "r") as file:
        configs = yaml.safe_load(file)
        tableau_configs = configs["tableau"]
        user_configs = configs["users"]

    server = TSC.Server(tableau_configs["server_url"])
    server.version = tableau_configs["version"]
    server.add_http_options({'verify': tableau_configs["verify"]})

    users = []

    with server.auth.sign_in(admin_auth(tableau_configs)):
        for user_name in user_configs:
            user_id = find_user_by_name(server, user_name)
            users.append({"username": user_name, "user_id": user_id})
            print(f"Found user with id {user_id}")

    for user in users:
        user_id = user["user_id"]
        user_name = user["username"]
        with server.auth.sign_in(create_tableau_auth_as_users(tableau_configs, user_id)):
            pat_response = create_pat_token(server)
            text_dump = create_text_dump(user_id, user_name, pat_response)
            with open("pat_tokens.txt", "a") as file:
                file.write(json.dumps(text_dump))
