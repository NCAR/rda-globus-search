import json
import os
from glob import glob

import click

from .auth import auth_client, internal_auth_client
from .search import search_client
from .database import get_dbconfigs, load_db, config_storage_adapter

# Output directories for extracted and assembled metadata
OUTPUT_BASE = '/glade/campaign/collections/rda/work/tcram/globus/search/dataset-metadata'

EXTRACTED_OUTPUT = os.path.join(OUTPUT_BASE, 'extracted')
ASSEMBLED_OUTPUT = os.path.join(OUTPUT_BASE, 'assembled')
TASK_SUBMIT_OUTPUT = os.path.join(OUTPUT_BASE, 'task_submit')
TASK_WATCH_OUTPUT = os.path.join(OUTPUT_BASE, 'task_watch')

TASK_OUTPUT_FILE = 'ingest-tasks.txt'

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
        return json.dump(obj.__dict__, fp, indent=2, separators=(",", ": "), ensure_ascii=False)
    return json.dumps(obj.__dict__, indent=2, separators=(",", ": "), ensure_ascii=False)

__all__ = (
    "EXTRACTED_OUTPUT",
    "ASSEMBLED_OUTPUT",
    "TASK_SUBMIT_OUTPUT",
    "TASK_WATCH_OUTPUT",
    "TASK_OUTPUT_FILE",
    "RDA_DOMAIN",
    "common_options",
    "all_filenames",
    "prettyprint_json",
    "config_storage_adapter",
    "internal_auth_client",
    "auth_client",
    "search_client",
    "get_dbconfigs",
    "load_db",
)