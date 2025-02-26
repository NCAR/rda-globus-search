import click
import logging
import logging.handlers

from . import extractor, assembler, submitter, watcher, manage_index, query
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
cli.add_command(extractor.extract_cli)
cli.add_command(assembler.assemble_cli)
cli.add_command(submitter.submit_cli)
cli.add_command(watcher.watch_cli)

# query results
cli.add_command(query.query_cli)
