import globus_sdk

from .auth import internal_auth_client

SEARCH_RESOURCE_SERVER = "search.api.globus.org"
SEARCH_SCOPES = "urn:globus:auth:scope:search.api.globus.org:all"

def search_client():
    authorizer = globus_sdk.ClientCredentialsAuthorizer(internal_auth_client(), SEARCH_SCOPES)
    return globus_sdk.SearchClient(authorizer=authorizer, app_name="dataset-search")