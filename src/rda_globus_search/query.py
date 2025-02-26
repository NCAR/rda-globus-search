import click
import globus_sdk

from .lib import common_options, prettyprint_json, search_client, config_storage_adapter


@click.command(
    "query",
    help="Perform a search query.\n"
    "This will automatically query the index created with create-index. "
    "This command supports various operations which are specific to the data "
    "generated by dataset-search, but the entire implementation is based  "
    "on standard features of the Globus Search service.\n"
    "You can use a query of '*' to match all data.",
)
@common_options
@click.argument("QUERY_STRING")
@click.option(
    "--limit", type=int, help="Limit the number of results to return", default=5
)
@click.option("--offset", type=int, help="Starting offset for paging", default=0)
@click.option(
    "--advanced",
    is_flag=True,
    help="Perform the search using the advanced query syntax",
)
@click.option(
    "--variables",
    help="Filter results to datasets matching any of the listed variables (comma-delimited). "
    "For example, '--variables=temperature,pressure'",
)
@click.option(
    "--keywords",
    help="Filter results to datasets matching any of the listed GCMD keywords (comma-delimited). "
    "For example, '--keywords=temperature,precipitation'",
)
@click.option(
    "--dump-query",
    is_flag=True,
    help="Write the query structure to stdout instead of submitting it to the service",
)
def query_cli(
    query_string,
    limit,
    offset,
    advanced,
    variables,
    keywords,
    dump_query,
):
    adapter = config_storage_adapter()
    client = search_client()
    index_info = adapter.read_config("index_info")
    if not index_info:
        raise click.UsageError("You must create an index with `create-index` first!")
    index_id = index_info["index_id"]

    query_obj = globus_sdk.SearchQuery(
        q=query_string, limit=limit, offset=offset, advanced=advanced
    )
    if variables:
        query_obj.add_filter("variables", variables.split(","), type="match_any")
    if keywords:
        query_obj.add_filter("gcmd_keywords", keywords.split(","), type="match_any")

    if dump_query:
        click.echo(prettyprint_json(dict(query_obj)))
    else:
        click.echo(prettyprint_json(client.post_search(index_id, query_obj).data))