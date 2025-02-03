import hashlib
import os
import shutil

import click

from .lib import common_options, prettyprint_json
from .lib.database import load_search_db
from rda_python_common.PgDBI import pgget

def get_search_metadata(dataset):
    """ Query and return search metadata """
    search_db = load_search_db()
    cond = "dsid='{}'".format(dataset)
    title = pgget('datasets', 'title', cond)
    data_types = pgget('data_types', 'DISTINCT(keyword) as data_type', cond)

    search_metadata = {}
    search_metadata.update(title)
    search_metadata.update(data_types)

    return search_metadata


def metadata2dict(dataset):
    """ Query metadata from the database and return in a comprehensive dict """

    search_metadata = get_search_metadata(dataset)
    #dssdb_metadata = get_dssdb_metadata(dataset)
    #wagtail_metadata = get_wagtail_metadata(dataset)

    metadata = {}
    metadata.update(search_metadata)

    return metadata

def target_file(output_directory, dataset):
    hashed_name = hashlib.sha256(dataset.encode("utf-8")).hexdigest()
    os.makedirs(output_directory, exist_ok=True)
    return os.path.join(output_directory, hashed_name) + ".json"



@click.command(
    "extract",
    help="Extract metadata from the database.\n"
    "This command creates dataset level metadata extracted from various metadata tables.",
)
@click.option(
    "--clean",
    default=False,
    is_flag=True,
    help="Empty the output directory before writing any data there",
)
@click.option(
    "--output",
    default="output/extracted",
    show_default=True,
    help="A path, relative to the current working directory, "
    "where the extracted metadata should be written",
)
@click.option(
    "--dataset",
    type=str,
    required=True,
    help="Dataset ID (dnnnnnn) to extract metadata.",
)
@common_options
def extract_cli(dataset, output, clean):
    if clean:
        shutil.rmtree(output, ignore_errors=True)

    rendered_data = {}
    rendered_data[dataset] = metadata2dict(dataset)

    for dataset, data in rendered_data.items():
        with open(target_file(output, dataset), "w") as fp:
            prettyprint_json(data, fp)

    click.echo("metadata extraction complete")
    click.echo(f"results visible in\n  {output}")
