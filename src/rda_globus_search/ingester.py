import click
from . import extractor, assembler, submitter, watcher

import logging
logger = logging.getLogger(__name__)

@click.command(
    help="Run all workflow commands in sequence: extract, assemble, submit, watch.  This is equivalent to running each subcommand (extract, assemble, submit, watch) individually."
)
@click.option(
    "--dsid",
    help="Dataset ID (dnnnnnn) to process."
)
@click.pass_context
def ingest(ctx, dsid):
    ctx = click.get_current_context()
    ctx.invoke(extractor.extract, dsid=dsid)
    ctx.invoke(assembler.assemble)
    ctx.invoke(submitter.submit)
    ctx.invoke(watcher.watch)
