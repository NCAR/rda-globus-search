import json
import os
import logging
from glob import glob
from io import StringIO
from html.parser import HTMLParser
import re

import click

from .auth import auth_client, internal_auth_client
from .search import search_client
from .database import get_dbconfigs, load_db, config_storage_adapter

GDEX_BASE_PATH = '/lustre/desc1/scratch'
LOGPATH = os.path.join(GDEX_BASE_PATH, 'tcram/logs/globus')

# Output directories for extracted and assembled metadata
OUTPUT_BASE = os.path.join(GDEX_BASE_PATH, 'tcram/globus/search/dataset-metadata')

EXTRACTED_OUTPUT = os.path.join(OUTPUT_BASE, 'extracted')
ASSEMBLED_OUTPUT = os.path.join(OUTPUT_BASE, 'assembled')
TASK_SUBMIT_OUTPUT = os.path.join(OUTPUT_BASE, 'task_submit')
TASK_WATCH_OUTPUT = os.path.join(OUTPUT_BASE, 'task_watch')

TASK_OUTPUT_FILE = 'ingest-tasks.txt'
GDEX_DOMAIN = "https://gdex.ucar.edu"

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

def validate_dsid(ctx, param, dsid):
    """ Validate dsid from command line input """
    ms = re.match(r'^([a-z]{1})(\d{3})(\d{3})$', dsid)
    if ms:
        return dsid
    else:
        raise click.BadParameter("format must be 'dnnnnnn'")

def prettyprint_json(obj, fp=None):
    if fp:
        return json.dump(obj, fp, indent=2, separators=(",", ": "), ensure_ascii=False)
    return json.dumps(obj, indent=2, separators=(",", ": "), ensure_ascii=False)

def configure_log():
   """ Congigure logging """
   logfile = os.path.join(LOGPATH, 'dataset-search.log')
   loglevel = 'INFO'
   format = '%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s'
   logging.basicConfig(filename=logfile, level=loglevel, format=format)

   return

def move_file_to_completed(output, filename):
    """
    Moves the file to a new subdirectory named 'completed'.
    If the file already exists in 'completed', appends an incrementing number to its name.
    Returns the new file path.
    """
    completed_dir = os.path.join(output, "completed")
    os.makedirs(completed_dir, exist_ok=True)
    base_name = os.path.basename(filename)
    dest_path = os.path.join(completed_dir, base_name)

    # Ensure unique filename in 'completed'
    base, ext = os.path.splitext(dest_path)
    counter = 0
    unique_dest = f"{base}_{counter}{ext}"
    counter += 1
    # Increment counter until a unique filename is found
    while os.path.exists(unique_dest):
        unique_dest = f"{base}_{counter}{ext}"
        counter += 1

    os.rename(filename, unique_dest)
    return unique_dest

class MLStripper(HTMLParser):
    """ 
    Class to strip HTML tags and characters from a string. 

    Example usage:
    s = MLStripper()
    s.feed(html_string)
    stripped_string = s.get_data()

    """
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.text = StringIO()
    def handle_data(self, d):
        self.text.write(d)
    def get_data(self):
        return self.text.getvalue()

def strip_html_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

__all__ = (
    "EXTRACTED_OUTPUT",
    "ASSEMBLED_OUTPUT",
    "TASK_SUBMIT_OUTPUT",
    "TASK_WATCH_OUTPUT",
    "TASK_OUTPUT_FILE",
    "GDEX_DOMAIN",
    "common_options",
    "all_filenames",
    "validate_dsid",
    "prettyprint_json",
    "configure_log",
    "move_file_to_completed",
    "config_storage_adapter",
    "strip_html_tags",
    "internal_auth_client",
    "auth_client",
    "search_client",
    "get_dbconfigs",
    "load_db",
)