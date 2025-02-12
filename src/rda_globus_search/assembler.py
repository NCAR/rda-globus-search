import json
import os
import shutil

import click

from .lib import (ASSEMBLED_OUTPUT, 
                  EXTRACTED_OUTPUT, 
                  all_filenames, 
                  common_options, 
                  prettyprint_json
)

MAX_BATCH_SIZE = 10

def build_entries(datafile):
    # read data
    with open(datafile) as fp:
        data = json.load(fp)

    entry_data = {k: v for k, v in data.items()}
    subject = entry_data['url']
    visibility = 'public'

    return [
        {
            "subject": subject,
            "visible_to": visibility,
            "content": entry_data,
        }
    ]

def flush_batch(entry_batch, docid, output_directory):
    os.makedirs(output_directory, exist_ok=True)
    fname = os.path.join(output_directory, f"ingest_doc_{docid}.json")
    with open(fname, "w") as fp:
        prettyprint_json(
            {"ingest_type": "GMetaList", "ingest_data": {"gmeta": entry_batch}}, fp
        )

@click.command(
    "assemble",
    help="Annotate data and prepare it for ingest.\n"
    "Given data from the Extractor, notate it and convert it into "
    "Ingest format. This will pull in data from the assembler configuration "
    "and use it to populate `visible_to` (see docs.globus.org/api/search for "
    "details on `visible_to`) and add fields to documents.",
)
@click.option(
    "--directory",
    default=EXTRACTED_OUTPUT,
    show_default=True,
    help="Absolute path to the directory "
    "containing extracted metadata for processing",
)
@click.option(
    "--clean",
    default=False,
    is_flag=True,
    help="Empty the output directory before writing any data there",
)
@click.option(
    "--output",
    default=ASSEMBLED_OUTPUT,
    show_default=True,
    help="Absolute path to the directory, "
    "where the assembled metadata should be written",
)
@common_options
def assemble_cli(directory, output, clean):
    if clean:
        shutil.rmtree(output, ignore_errors=True)

    entry_docs = []
    for filename in all_filenames(directory, '*.json'):
        entry_docs.extend(build_entries(filename))

    current_doc_id = 0
    batch = []
    for entry in entry_docs:
        if len(batch) >= MAX_BATCH_SIZE:
            flush_batch(batch, current_doc_id, output)
            batch = []
            current_doc_id += 1
        batch.append(entry)
    flush_batch(batch, current_doc_id, output)

    click.echo("ingest document assembly complete")
    click.echo(f"results visible in\n  {output}")