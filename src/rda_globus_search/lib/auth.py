import yaml
import globus_sdk
from globus_sdk.tokenstorage import SQLiteAdapter

AUTH_RESOURCE_SERVER = "auth.globus.org"
AUTH_SCOPES = ["openid", "profile"]

CLIENT_CONFIG = '/glade/u/home/rdadata/.globusconfig.yml'
TOKEN_STORAGE = '/glade/u/home/rdadata/.globus_searchable_datasets.db'

def get_client_credentials():
    """ Get Globus Search service client ID and secret """
    with open(CLIENT_CONFIG) as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(e)

def internal_auth_client():
    client_config = get_client_credentials()['search_client']
    client_id = client_config['client_id']
    client_secret = client_config['client_secret']
    return globus_sdk.ConfidentialAppAuthClient(client_id, client_secret, app_name="dataset-search")

def token_storage_adapter():
    if not hasattr(token_storage_adapter, "_instance"):
        # namespace is equal to the current environment
        token_storage_adapter._instance = SQLiteAdapter(TOKEN_STORAGE, namespace="DEFAULT")
    return token_storage_adapter._instance

def auth_client():
    authorizer = globus_sdk.ClientCredentialsAuthorizer(internal_auth_client(), AUTH_SCOPES)
    return globus_sdk.AuthClient(authorizer=authorizer, app_name="dataset-search")