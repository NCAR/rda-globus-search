import json
import os
import shutil

import click

from .lib import (
    all_filenames, 
    common_options, 
    search_client, 
    config_storage_adapter, 
    move_file_to_completed,
    ASSEMBLED_OUTPUT,
    TASK_SUBMIT_OUTPUT,
    TASK_OUTPUT_FILE,
)

import logging
logger = logging.getLogger(__name__)

def submit_doc(client, index_id, filename, task_list_file):
    with open(filename) as fp:
        data = json.load(fp)
    res = client.ingest(index_id, data)
    with open(task_list_file, "a") as fp:
        fp.write(res["task_id"] + "\n")


@click.command(
    "submit",
    help="Submit ingest documents as new Globus Search ingest tasks.\n"
    "Read ingest documents produced by the Assembler, submit them "
    "each as a new ingest task and log their ingest task IDs. "
    "These tasks can then be monitored with the `watch` command.",
)
@click.option(
    "--directory",
    default=ASSEMBLED_OUTPUT,
    show_default=True,
    help="Absolute path to the directory containing "
    "assembled ingest documents to submit",
)
@click.option(
    "--output",
    default=TASK_SUBMIT_OUTPUT,
    show_default=True,
    help="Abolute path to the directory "
    "where the resulting task IDs will be written",
)
@click.option(
    "--index-id",
    default=None,
    help="Override the default search index ID where the tasks should be submitted. "
    "If omitted, the index stored in the sqlite3 configuration database, or "
    "the index created with `create-index` will be used.",
)
@common_options
def submit_cli(directory, output, index_id):
    client = search_client()

    os.makedirs(output, exist_ok=True)
    task_list_file = os.path.join(output, TASK_OUTPUT_FILE)

    # move any existing task list file to the 'completed' subdirectory
    if os.path.exists(task_list_file):
        moved_task_file = move_file_to_completed(output, task_list_file)
        click.echo(f"Moved existing task list file to {moved_task_file}")

    with open(task_list_file, "w"):  # create a new empty task list file
        pass

    if not index_id:
        index_info = config_storage_adapter().read_config("index_info")
        if index_info is None:
            raise click.UsageError(
                "Cannot submit without first setting up "
                "an index or passing '--index-id'"
            )
        index_id = index_info["index_id"]

    for filename in all_filenames(directory):
        submit_doc(client, index_id, filename, task_list_file)

    click.echo(
        f"""\
ingest document submission (task submission) complete
task IDs are visible in
    {task_list_file}"""
    )