import os

import click
import globus_sdk
from globus_sdk.tokenstorage import SimpleJSONFileAdapter

AUTH_RESOURCE_SERVER = "auth.globus.org"

CLIENT_ID = "48ea646d-9628-4363-baa7-d22701b23233"
TOKEN_FILE = "/glade/u/home/rdadata/lib/python/globus_rda_search_tokens.json"

def internal_auth_client():
    return globus_sdk.NativeAppAuthClient(CLIENT_ID, app_name="dataset-search")


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