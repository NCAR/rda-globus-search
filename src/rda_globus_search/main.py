import click
import logging
import logging.handlers

from . import (
    extractor, 
    assembler, 
    submitter, 
    watcher,
    ingester, 
    manage_index, 
    manage_subject,
    query
)
from .lib import common_options, configure_log

logger = logging.getLogger(__name__)
configure_log()

@click.group("dataset-search")
@common_options
def cli():
    pass

# index management
manage_index.add_commands(cli)

# cli workflow
cli.add_command(extractor.extract)
cli.add_command(assembler.assemble)
cli.add_command(submitter.submit)
cli.add_command(watcher.watch)
cli.add_command(ingester.ingest)

# query results
cli.add_command(query.query)

# subject management
manage_subject.add_commands(cli)
