import globus_sdk
from globus_sdk.tokenstorage import SimpleJSONFileAdapter

AUTH_RESOURCE_SERVER = "auth.globus.org"

CLIENT_CONFIG = '/glade/u/home/rdadata/.globusconfig.yml'

def get_client_credentials():
    """ Get Globus Search service client ID and secret """
    with open(CLIENT_CONFIG) as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(e)

def internal_auth_client():
    client_config = get_client_credentials['search_client']
    client_id = client_config['client_id']
    client_secret = client_config['client_secret']
    return globus_sdk.ConfidentialAppAuthClient(client_id, client_secret, app_name="dataset-search")

def token_storage_adapter():
    if not hasattr(token_storage_adapter, "_instance"):
        # namespace is equal to the current environment
        token_storage_adapter._instance = SimpleJSONFileAdapter(TOKEN_FILE)
    return token_storage_adapter._instance


def auth_client():
    storage_adapter = token_storage_adapter()

    authdata = storage_adapter.get_token_data(AUTH_RESOURCE_SERVER)
    access_token = authdata["access_token"]
    refresh_token = authdata["refresh_token"]
    access_token_expires = authdata["expires_at_seconds"]

    authorizer = globus_sdk.RefreshTokenAuthorizer(
        refresh_token,
        internal_auth_client(),
        access_token,
        int(access_token_expires),
        on_refresh=storage_adapter.on_refresh,
    )

    return globus_sdk.AuthClient(authorizer=authorizer, app_name="dataset-search")