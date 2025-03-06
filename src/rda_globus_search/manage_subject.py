import os
import click

from .lib import (
    common_options,
    validate_dsid,
    search_client,
    config_storage_adapter,
    OUTPUT_BASE,
)
from .extractor import get_other_metadata
from globus_sdk import GlobusAPIError

DELETE_TASK_OUTPUT = os.path.join(OUTPUT_BASE, "task_delete")

import logging
logger = logging.getLogger(__name__)

def submit_delete_subject(dsid, index_id, task_list_file):
    subject = get_other_metadata(dsid)['url']

    client = search_client()

    try:
        res = client.delete_subject(index_id, subject)
    except GlobusAPIError as e:
        msg = ("Globus API Error when attempting to delete a search index subject:\n"
            "HTTP status: {0}\n"
            "Error code: {1}\n"
            "Error message: {2}\n"
            "Search index: {3}\n"
            "dsid: {4}\n"
            "subject: {5}".format(e.http_status, e.code, e.message, index_id, dsid, subject)
        )
        logger.error(msg)

    task_id = res["task_id"]

    with open(task_list_file, "a") as fp:
        fp.write(task_id + "\n")

    logger.info(f"""\
                delete subject task for dsid = {dsid} 
                submitted as task ID {task_id}""")

    return task_id

@click.command(
    "delete-subject",
    help="Delete a subject document from a search index."
)
@click.option(
    "--dsid",
    type=str,
    required=True,
    callback=validate_dsid,
    help="Dataset ID (dnnnnnn) corresponding to the subject to delete from the search index.",
)
@click.option(
    "--index-id",
    default=None,
    help="Override the default search index ID where the subject should be deleted. "
    "If omitted, the index stored in the sqlite3 configuration database, or "
    "the index created with `create-index` will be used.",
)
@common_options
def delete_subject(dsid, index_id):
    os.makedirs(DELETE_TASK_OUTPUT, exist_ok=True)
    task_list_file = os.path.join(DELETE_TASK_OUTPUT, "delete-tasks.txt")

    with open(task_list_file, "w"):  # empty the file (open in write mode)
        pass

    if not index_id:
        index_info = config_storage_adapter().read_config("index_info")
        if index_info is None:
            raise click.UsageError(
                "Cannot delete subject without first setting up "
                "an index or passing '--index-id'"
            )
        index_id = index_info["index_id"]

    task_id = submit_delete_subject(dsid, index_id, task_list_file)

    click.echo(
        f"""\
subject delete (task submission) for dsid = {dsid} complete
task ID {task_id} written to
    {task_list_file}"""
    )

def add_commands(group):
    group.add_command(delete_subject)
