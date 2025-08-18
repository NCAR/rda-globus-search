import click
from . import extractor, assembler, submitter, watcher
from .lib import common_options, validate_dsid

import logging
logger = logging.getLogger(__name__)

@click.command(
    help="Run all workflow commands in sequence: extract, assemble, submit, watch.  This is equivalent to running each subcommand (extract, assemble, submit, watch) individually."
)
@click.option(
    "--dsid",
    help="Dataset ID (dnnnnnn) to process.",
    required=True,
    callback=validate_dsid,
)
@click.option(
    "--clean",
    default=False,
    is_flag=True,
    help="Empty the output directory before writing any data there.",
)
@common_options
@click.pass_context
def ingest(ctx, dsid, clean):
    ctx = click.get_current_context()
    ctx.invoke(extractor.extract, dsid=dsid, clean=clean)
    ctx.invoke(assembler.assemble, clean=clean)
    ctx.invoke(submitter.submit)
    ctx.invoke(watcher.watch)
