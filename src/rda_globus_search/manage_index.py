import click

from .lib import (
    auth_client,
    common_options,
    prettyprint_json,
    search_client,
    config_storage_adapter,
)


@click.command(
    "create-index",
    help="Create a search index for searchable RDA datasets."
)
@click.option(
    "--name",
    type=str,
    required=True,
    help="Display name for the search index.",
)
@click.option(
    "--description",
    type=str,
    required=True,
    help="General description of the search index.",
)
@common_options
def create_index(name, description):
    adapter = config_storage_adapter()
    client = search_client()

    userinfo = auth_client().oauth2_userinfo()
    username = userinfo["preferred_username"]

    # Check if index already exists
    indices = [si for si in client.get("/v1/index_list").data['index_list']
        if si['display_name'] == name
        and 'owner' in si['permissions']
        ]
    if indices:
        index_id = indices[0]
        click.echo(f"Index already exists, id='{index_id}'.")
        click.echo("Use the subcommand 'set-index' to set the index for 'submit' and 'query' commands.")
    else:
        index = client.create_index(display_name=name, description=description).data
        index_id = index["id"]
        adapter.store_config("index_info", {"index_id": index_id, 
                                            "display name": name,
                                            "description": description,
                                            "created by": username})
        click.echo(f"successfully created index, id='{index_id}'")

@click.command(
    "show-index",
    help="Show index info.\n"
    "Detailed info about the RDA searchable datasets index. "
    "Must run after create-index.\n"
    "The data is verbatim output from the Globus Search API.",
)
@common_options
def show_index():
    adapter = config_storage_adapter()
    client = search_client()

    index_info = adapter.read_config("index_info")
    if not index_info:
        raise click.UsageError("You must create an index with `create-index` first!")
    index_id = index_info["index_id"]

    res = client.get(f"/v1/index/{index_id}")
    click.echo(prettyprint_json(res.data))


@click.command(
    "set-index",
    help="Set the Index for searchable datasets.\n"
    "If an index has already been created, either via a previous use of "
    "`create-index` or by another means, this command allows you to set the "
    "default index for commands like `submit` and `query`.",
)
@common_options
@click.argument("INDEX_ID", type=click.UUID)
def set_index(index_id):
    adapter = config_storage_adapter()
    adapter.store_config("index_info", {"index_id": str(index_id)})
    click.echo(f"successfully updated configured index, id='{index_id}'")


def add_commands(group):
    group.add_command(create_index)
    group.add_command(show_index)
    group.add_command(set_index)