import os
import shutil

import click

from .lib import (
    EXTRACTED_OUTPUT, 
    RDA_DOMAIN, 
    common_options, 
    validate_dsid,
    prettyprint_json, 
    strip_html_tags
)
from .lib.database import load_db
from rda_python_common.PgDBI import pgget, pgmget

import logging
logger = logging.getLogger(__name__)

def get_search_metadata(dsid):
    """ Query and return search metadata """

    load_db('search')
    cond = f"dsid='{dsid}'"
    search_metadata = {}

    # Dataset type ('P' or 'H', public or historical).  Other dataset types
    # should be ignored and not ingested ('W', 'I', etc.).
    dataset_type = pgget('datasets', 'type', cond)
    if dataset_type['type'] not in ['P', 'H']:
        logger.warning(f"Dataset type '{dataset_type['type']}' is not supported for dsid {dsid} (it must be 'P' or 'H').")
        logger.warning("Skipping metadata extraction for this dataset.")
        click.echo(f"Dataset type '{dataset_type['type']}' is not supported for dsid {dsid} (it must be 'P' or 'H').")
        click.echo("Skipping metadata extraction for this dataset.")
        raise click.Abort()
    else:
        search_metadata.update({'dataset_type': dataset_type['type']})

    # Dataset title and summary
    myrec = pgget('datasets', 'title, summary', cond)
    description = strip_html_tags(myrec['summary'])
    search_metadata.update({'title': myrec['title'], 'description': description})

    # Data type (grid, platform_observation, etc.)
    data_types = pgmget('data_types', 'DISTINCT(keyword) as data_type', cond)
    search_metadata.update({'data_type': data_types['data_type']})

    # GCMD variables
    keyword_query = f"SELECT " \
        f"CONCAT('EARTH SCIENCE > ', topic, ' > ', term, ' > ', keyword) " \
        f"AS keywords " \
        f"FROM gcmd_variables WHERE dsid='{dsid}'"
    gcmd_keywords = pgmget(None, None, keyword_query)
    search_metadata.update({'gcmd_keywords': gcmd_keywords['keywords']})

    # Time resolutions
    time_resolutions = pgmget('time_resolutions', 'DISTINCT(keyword) as time_resolutions', cond)
    if not time_resolutions:
        search_metadata.update({'time_resolution': None})
    else:
        search_metadata.update({'time_resolution': time_resolutions['time_resolutions']})

    # Platforms
    platform_query = f"SELECT path " \
        f"FROM platforms_new AS p " \
        f"LEFT JOIN gcmd_platforms AS g " \
        f"ON g.uuid = p.keyword " \
        f"WHERE p.dsid = '{dsid}'"
    platforms = pgmget(None, None, platform_query)
    if not platforms:
        search_metadata.update({'platform': None})
    else:
        search_metadata.update({'platform': platforms['path']})

    # Grid resolutions (gridded datasets only)
    grid_resolutions = pgmget('grid_resolutions', 'keyword', cond)
    if not grid_resolutions:
        search_metadata.update({'spatial_resolution': None})
    else:
        search_metadata.update({'spatial_resolution': grid_resolutions['keyword']})
    
    # ISO topic
    topic = pgget('topics', 'keyword', cond)
    search_metadata.update({'topic': topic['keyword']})

    # "Collected from" projects (projects that produced the data)
    project_query = f"SELECT path " \
        f"FROM projects_new AS p " \
        f"LEFT JOIN gcmd_projects AS g " \
        f"ON g.uuid = p.keyword " \
        f"WHERE p.dsid = '{dsid}'"
    projects = pgmget(None, None, project_query)
    if not projects:
        search_metadata.update({'project': None})
    else:
        search_metadata.update({'project': projects['path']})

    # Projects supported by the data
    supported_projects_query = f"SELECT path " \
        f"FROM supported_projects AS p " \
        f"LEFT JOIN gcmd_projects AS g " \
        f"ON g.uuid = p.keyword " \
        f"WHERE p.dsid = '{dsid}'"
    supported_projects = pgmget(None, None, supported_projects_query)
    if not supported_projects:
        search_metadata.update({'supports_project': None})
    else:
        search_metadata.update({'supports_project': supported_projects['path']})

    # Data formats
    formats = pgmget('formats', 'DISTINCT(keyword) as format', cond)
    if not formats:
        search_metadata.update({'format': None})
    else:
        search_metadata.update({'format': formats['format']})

    # Observation instruments
    instruments_query = f"SELECT path " \
        f"FROM instruments AS i " \
        f"LEFT JOIN gcmd_instruments AS g " \
        f"ON g.uuid = i.keyword " \
        f"WHERE i.dsid = '{dsid}'"
    instruments = pgmget(None, None, instruments_query)
    if not instruments:
        search_metadata.update({'instrument': None})
    else:
        search_metadata.update({'instrument': instruments['path']})
    
    # GCMD data locations
    location_query = f"SELECT path " \
        f"FROM locations_new AS l " \
        f"LEFT JOIN gcmd_locations AS g " \
        f"ON g.uuid = l.keyword " \
        f"WHERE l.dsid = '{dsid}'"
    locations = pgmget(None, None, location_query)
    if not locations:
        search_metadata.update({'location': None})
    else:
        search_metadata.update({'location': locations['path']})
    
    # Data contributors
    contributor_query = f"SELECT path " \
        f"FROM contributors_new AS c " \
        f"LEFT JOIN gcmd_providers AS g " \
        f"ON g.uuid = c.keyword " \
        f"WHERE c.dsid = '{dsid}'"
    contributors = pgmget(None, None, contributor_query)
    if not contributors:
        search_metadata.update({'data_contributors': None})
    else:
        search_metadata.update({'data_contributors': contributors['path']})

    return search_metadata

def get_dssdb_metadata(dsid):
    """ Query and return metadata from dssdb tables """

    load_db('dssdb')
    cond = f"dsid='{dsid}'"
    dssdb_metadata = {}

    doi = pgget('dsvrsn', 'doi', cond + " AND status='A'")
    if doi and 'doi' in doi:
        dssdb_metadata.update({'doi': doi['doi']})
    else:
        dssdb_metadata.update({'doi': None})

    dsperiod_query = f"SELECT " \
        f"MIN(CONCAT(date_start, ' ', time_start)) AS date_start, " \
        f"MAX(CONCAT(date_end, ' ', time_end)) AS date_end " \
        f"FROM dsperiod " \
        f"WHERE {cond}"
    dsperiod = pgmget(None, None, dsperiod_query)
    dssdb_metadata.update({'temporal_range_start': dsperiod['date_start'][0],
                           'temporal_range_end': dsperiod['date_end'][0]})

    return dssdb_metadata

def get_wagtail_metadata(dsid):
    """ Query and return wagtail metadata """

    load_db('wagtail')
    cond = f"dsid='{dsid}'"
    wagtail_metadata = {}

    wagtail_rec = pgget('dataset_description_datasetdescriptionpage', 'update_freq, variables, volume', cond)

    if 'update_freq' not in wagtail_rec or wagtail_rec['update_freq'] is None:
        updates = None
    else:
        updates = wagtail_rec['update_freq']

    if 'variables' not in wagtail_rec or wagtail_rec['variables'] is None:
        variables = None
    else:
        variables = wagtail_rec['variables']['gcmd'] if 'gcmd' in wagtail_rec['variables'] else None

    if 'volume' not in wagtail_rec or wagtail_rec['volume'] is None:
        total_volume = None
    else:
        total_volume = wagtail_rec['volume']['full'] if 'full' in wagtail_rec['volume'] else None

    wagtail_metadata.update({
        'updates': updates,
        'variables': variables,
        'total_volume': total_volume
    })

    return wagtail_metadata

def get_other_metadata(dsid):
    """ Returns metadata not stored in DB """

    other_metadata = {}
    url = os.path.join(RDA_DOMAIN, 'datasets', dsid)

    other_metadata.update({'dataset_id': dsid,
                           'url': url})

    return other_metadata

def metadata2dict(dsid):
    """ Query metadata from the database and return in a comprehensive dict """

    metadata = {}
    metadata.update(get_search_metadata(dsid))
    metadata.update(get_dssdb_metadata(dsid))
    metadata.update(get_wagtail_metadata(dsid))
    metadata.update(get_other_metadata(dsid))

    return metadata

def target_file(output_directory, dsid):
    target_name = f"{dsid}.search-metadata"
    os.makedirs(output_directory, exist_ok=True)
    return os.path.join(output_directory, target_name) + ".json"

@click.command(
    "extract",
    help="Extract metadata from the database.\n"
    "This command creates dataset level metadata extracted from various metadata tables.",
)
@click.option(
    "--dsid",
    type=str,
    required=True,
    callback=validate_dsid,
    help="Dataset ID (dnnnnnn) to extract metadata.",
)
@click.option(
    "--clean",
    default=False,
    is_flag=True,
    help="Empty the output directory before writing any data there.",
)
@click.option(
    "--output",
    default=EXTRACTED_OUTPUT,
    show_default=True,
    help="Absolute path where the extracted metadata should be written.",
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

    logger.info(f"metadata extraction complete for dsid {dsid}")
    click.echo("metadata extraction complete")
    click.echo(f"results visible in\n  {target_file(output, dsid)}")
