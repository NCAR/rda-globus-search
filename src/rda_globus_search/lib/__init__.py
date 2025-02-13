import json
import os
from glob import glob

import click

from .auth import auth_client, internal_auth_client, token_storage_adapter
from .search import search_client
from .database import get_dbconfigs, load_db

APP_SCOPES = ["openid", "profile", "urn:globus:auth:scope:search.api.globus.org:all"]

# Output directories for extracted and assembled metadata
EXTRACTED_OUTPUT = '/glade/campaign/collections/rda/work/tcram/globus/search/dataset-metadata/extracted'
ASSEMBLED_OUTPUT = '/glade/campaign/collections/rda/work/tcram/globus/search/dataset-metadata/assembled'

RDA_DOMAIN = "https://rda.ucar.edu"

def common_options(f):
    # any shared/common options for all commands
    return click.help_option("-h", "--help")(f)

def all_filenames(directory, pattern=None):
    """ 
    Returns the absolute path and file name to all files 
    in the given directory.

    Pattern can be a specific or wild-card file name to be
    appended to directory in the search, e.g.
    pattern = '*.json' will return only json files in 
    the directory.
    """

    if pattern:
        for f in glob(os.path.join(directory, pattern)):
            yield f
    else:
        for dirpath, _dirnames, filenames in os.walk(directory):
            for f in filenames:
                yield os.path.join(dirpath, f)

def prettyprint_json(obj, fp=None):
    if fp:
        return json.dump(obj, fp, indent=2, separators=(",", ": "), ensure_ascii=False)
    return json.dumps(obj, indent=2, separators=(",", ": "), ensure_ascii=False)

__all__ = (
    "APP_SCOPES",
    "EXTRACTED_OUTPUT",
    "ASSEMBLED_OUTPUT",
    "RDA_DOMAIN",
    "common_options",
    "all_filenames",
    "prettyprint_json",
    "token_storage_adapter",
    "internal_auth_client",
    "auth_client",
    "search_client",
    "get_dbconfigs",
    "load_db",
)