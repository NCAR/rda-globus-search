import os
import time

import click

from .lib import (
    common_options, 
    search_client,
    TASK_SUBMIT_OUTPUT,
    TASK_WATCH_OUTPUT,
    TASK_OUTPUT_FILE,
)

import logging
logger = logging.getLogger(__name__)

def wait(client, task_id, max_wait):
    waited = 0
    while True:
        res = client.get_task(task_id)
        if res["state"] in ("SUCCESS", "FAILED"):
            task_info = {
                "task_id": task_id,
                "state": res["state"],
                "index_id": res["index_id"],
                "creation_date": res["creation_date"],
                "completion_date": res.get("completion_date", ""),
                "message": res.get("message", ""),
                "additional_details": res.get("additional_details", {}),
            }
            return task_info

        # wait 1s and check for timeout
        waited += 1
        if waited >= max_wait:
            task_info = {
                "task_id": task_id,
                "state": res.get("state", "UNKNOWN"),
                "index_id": res["index_id"],
                "creation_date": res["creation_date"],
                "completion_date": res.get("completion_date", ""),
                "message": res.get("message", ""),
                "additional_details": res.get("additional_details", {}),
            }
            return task_info
        time.sleep(1)


@click.command(
    "watch",
    help="Wait for Globus Search ingest tasks to complete.\n"
    "Wait for Tasks whose IDs are listed in the ingest task ID file to complete, and "
    "print information about the number which succeed or fail. If a task takes too "
    "long, it is treated as a failure.",
)
@click.option(
    "--task-id-file",
    default=os.path.join(TASK_SUBMIT_OUTPUT, TASK_OUTPUT_FILE),
    show_default=True,
    help="Absolute path to the file containing task IDs to watch",
)
@click.option(
    "--output",
    default=TASK_WATCH_OUTPUT,
    show_default=True,
    help="Absolute path to the directory, "
    "where the task status information will be written",
)
@click.option(
    "--max-wait",
    default=10,
    show_default=True,
    type=int,
    help="The maximum amount of time (in seconds) to wait for a task to complete before "
    "assuming that it is failed.",
)
@click.option(  # for easy testing of the progress bar, sleep between tasks
    "--delay", hidden=True, type=float
)
@common_options
def watch_cli(task_id_file, output, max_wait, delay):
    client = search_client()

    task_ids = set()
    with open(task_id_file) as fp:
        for line in fp:
            line = line.strip()
            if line:  # skip empty
                task_ids.add(line.strip())

    results = []
    with click.progressbar(task_ids) as bar:
        for task_id in bar:
            task_info = wait(client, task_id, max_wait)
            results.append(task_info)
            if delay is not None:
                time.sleep(delay)

    n = len(results)
    if all(task["state"] == "SUCCESS" for task in results):
        click.echo(f"All tasks completed successfully ({n}/{n})")
    else:
        num_success = len([x for x in results if x["state"] == "SUCCESS"])
        num_fail = n - num_success
        click.echo(f"{num_success} tasks completed successfully ({num_success}/{n})")
        click.echo(f"{num_fail} tasks failed or did not complete ({num_fail}/{n})")

        failed_tasks = [x for x in results if x["state"] != "SUCCESS"]
        if failed_tasks:
            click.echo("Failed tasks:")
            for task in failed_tasks:
                click.echo(f"  Task ID: {task['task_id']}")
                click.echo(f"    State: {task['state']}")
                click.echo(f"    Message: {task['message']}")
                click.echo(f"    Additional Details: {task['additional_details']}")
                
    # Write results to output file
    os.makedirs(output, exist_ok=True)
    output_file = os.path.join(output, "task_results.json")
    with open(output_file, "w") as fp:
        import json
        json.dump(results, fp, indent=2)