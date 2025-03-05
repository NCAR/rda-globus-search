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

DELETE_TASK_OUTPUT = os.path.join(OUTPUT_BASE, "task_delete")

import logging
logger = logging.getLogger(__name__)

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
    adapter = config_storage_adapter()
    client = search_client()

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

    subject = get_other_metadata(dsid)['url']

    res = client.delete_subject(index_id, subject)
    task_id = res["task_id"]

    with open(task_list_file, "a") as fp:
        fp.write(task_id + "\n")

    logger.info(f"""\
                delete subject task for dsid = {dsid} 
                submitted as task ID {task_id}""")

    click.echo(
        f"""\
subject delete (task submission) for dsid = {dsid} complete
task ID {task_id} written to
    {task_list_file}"""
    )

def add_commands(group):
    group.add_command(delete_subject)
