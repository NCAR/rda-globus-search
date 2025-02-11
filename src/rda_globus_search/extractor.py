import hashlib
import os
import shutil

import click

from .lib import common_options, prettyprint_json
from .lib.database import load_db, load_search_db, load_dssdb_db, load_wagtail_db
from rda_python_common.PgDBI import pgget, pgmget

def get_search_metadata(dsid):
    """ Query and return search metadata """

    load_db('search')
    cond = "dsid='{}'".format(dsid)
    search_metadata = {}

    # Dataset title and summary
    myrec = pgget('datasets', 'title, summary', cond)
    search_metadata.update({'title': myrec['title'], 'description': myrec['summary']})

    # Data type (grid, platform_observation, etc.)
    data_types = pgmget('data_types', 'DISTINCT(keyword) as data_type', cond)
    search_metadata.update({'data type': data_types['data_type']})

    # GCMD variables
    keyword_query = "SELECT " \
        "CONCAT('EARTH SCIENCE > ', topic, ' > ', term, ' > ', keyword) " \
        "AS keywords " \
        "FROM gcmd_variables WHERE dsid='{}'".format(dsid)
    gcmd_keywords = pgmget(None, None, keyword_query)
    search_metadata.update({'GCMD keywords': gcmd_keywords['keywords']})

    # Time resolutions
    time_resolutions = pgmget('time_resolutions', 'DISTINCT(keyword) as time_resolutions', cond)
    if not time_resolutions:
        search_metadata.update({'time resolution': None})
    else:
        search_metadata.update({'time resolution': time_resolutions['time_resolutions']})

    # Platforms
    platform_query = "SELECT path " \
        "FROM platforms_new AS p " \
        "LEFT JOIN gcmd_platforms AS g " \
        "ON g.uuid = p.keyword " \
        "WHERE p.dsid = '{}'".format(dsid)
    platforms = pgmget(None, None, platform_query)
    if not platforms:
        search_metadata.update({'platform': None})
    else:
        search_metadata.update({'platform': platforms['path']})

    # Grid resolutions (gridded datasets only)
    grid_resolutions = pgmget('grid_resolutions', 'keyword', cond)
    if not grid_resolutions:
        search_metadata.update({'spatial resolution': None})
    else:
        search_metadata.update({'spatial resolution': grid_resolutions['keyword']})
    
    # ISO topic
    topic = pgget('topics', 'keyword', cond)
    search_metadata.update({'topic': topic['keyword']})

    # "Collected from" projects (projects that produced the data)
    project_query = "SELECT path " \
        "FROM projects_new AS p " \
        "LEFT JOIN gcmd_projects AS g " \
        "ON g.uuid = p.keyword " \
        "WHERE p.dsid = '{}'".format(dsid)
    projects = pgmget(None, None, project_query)
    if not projects:
        search_metadata.update({'project': None})
    else:
        search_metadata.update({'project': projects['path']})

    # Projects supported by the data
    supported_projects_query = "SELECT path " \
        "FROM supported_projects AS p " \
        "LEFT JOIN gcmd_projects AS g " \
        "ON g.uuid = p.keyword " \
        "WHERE p.dsid = '{}'".format(dsid)
    supported_projects = pgmget(None, None, supported_projects_query)
    if not supported_projects:
        search_metadata.update({'supports project': None})
    else:
        search_metadata.update({'supports project': supported_projects['path']})

    # Data formats
    formats = pgmget('formats', 'DISTINCT(keyword) as format', cond)
    if not formats:
        search_metadata.update({'format': None})
    else:
        search_metadata.update({'format': formats['format']})

    # Observation instruments
    instruments_query = "SELECT path " \
        "FROM instruments AS i " \
        "LEFT JOIN gcmd_instruments AS g " \
        "ON g.uuid = i.keyword " \
        "WHERE i.dsid = '{}'".format(dsid)
    instruments = pgmget(None, None, instruments_query)
    if not instruments:
        search_metadata.update({'instrument': None})
    else:
        search_metadata.update({'instrument': instruments['path']})
    
    # GCMD data locations
    location_query = "SELECT path " \
        "FROM locations_new AS l " \
        "LEFT JOIN gcmd_locations AS g " \
        "ON g.uuid = l.keyword " \
        "WHERE l.dsid = '{}'".format(dsid)
    locations = pgmget(None, None, location_query)
    if not locations:
        search_metadata.update({'location': None})
    else:
        search_metadata.update({'location': locations['path']})
    
    # Data contributors
    contributor_query = "SELECT path " \
        "FROM contributors_new AS c " \
        "LEFT JOIN gcmd_providers AS g " \
        "ON g.uuid = c.keyword " \
        "WHERE c.dsid = '{}'".format(dsid)
    contributors = pgmget(None, None, contributor_query)
    if not contributors:
        search_metadata.update({'data contributors': None})
    else:
        search_metadata.update({'data contributors': contributors['path']})

    return search_metadata

def get_dssdb_metadata(dsid):
    """ Query and return metadata from dssdb tables """

    load_db('dssdb')
    cond = "dsid='{}'".format(dsid)
    dssdb_metadata = {}

    doi = pgget('dsvrsn', 'doi', cond + " AND status='A'")
    dssdb_metadata.update({'doi': doi})

    dsperiod_query = "SELECT " \
        "MIN(CONCAT(date_start, ' ', time_start)) AS date_start, " \
        "MAX(CONCAT(date_end, ' ', time_end)) AS date_end " \
        "FROM dsperiod " \
        "WHERE {}".format(cond)
    dsperiod = pgmget(None, None, dsperiod_query)
    dssdb_metadata.update({'temporal range start': dsperiod['date_start'],
                           'temporal range end': dsperiod['date_end']})

    return dssdb_metadata

def get_wagtail_metadata(dsid):
    """ Query and return wagtail metadata """

    load_db('wagtail')
    cond = "dsid='{}'".format(dsid)
    wagtail_metadata = {}

    wagtail_rec = pgget('dataset_description_datasetdescriptionpage', 'update_freq, variables, volume', cond)

    updates = wagtail_rec['update_freq']
    variables = wagtail_rec['variables']['gcmd']
    total_volume = wagtail_rec['volume']['full']

    wagtail_metadata.update({
        'updates': updates,
        'variables': variables,
        'total volume': total_volume
    })

    return wagtail_metadata

def metadata2dict(dsid):
    """ Query metadata from the database and return in a comprehensive dict """

    search_metadata = get_search_metadata(dsid)
    dssdb_metadata = get_dssdb_metadata(dsid)
    wagtail_metadata = get_wagtail_metadata(dsid)

    metadata = {}
    metadata.update(search_metadata)
    metadata.update(dssdb_metadata)
    metadata.update(wagtail_metadata)

    return metadata

def target_file(output_directory, dsid):
    hashed_name = hashlib.sha256(dsid.encode("utf-8")).hexdigest()
    os.makedirs(output_directory, exist_ok=True)
    return os.path.join(output_directory, hashed_name) + ".json"



@click.command(
    "extract",
    help="Extract metadata from the database.\n"
    "This command creates dataset level metadata extracted from various metadata tables.",
)
@click.option(
    "--dsid",
    type=str,
    required=True,
    help="Dataset ID (dnnnnnn) to extract metadata.",
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
@common_options
def extract_cli(dsid, output, clean):
    if clean:
        shutil.rmtree(output, ignore_errors=True)

    rendered_data = {}
    rendered_data[dsid] = metadata2dict(dsid)

    for dsid, data in rendered_data.items():
        with open(target_file(output, dsid), "w") as fp:
            prettyprint_json(data, fp)

    click.echo("metadata extraction complete")
    click.echo(f"results visible in\n  {output}")
