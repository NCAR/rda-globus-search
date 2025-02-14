import globus_sdk

from .auth import internal_auth_client
from .lib import APP_SCOPES

SEARCH_RESOURCE_SERVER = "search.api.globus.org"

def search_client():
    authorizer = globus_sdk.ClientCredentialsAuthorizer(internal_auth_client, APP_SCOPES)
    return globus_sdk.SearchClient(authorizer=authorizer, app_name="dataset-search")