import yaml
from rda_python_common.PgDBI import default_scinfo

DATABASE_CONFIG = '/glade/u/home/rdadata/.pgconfig.yml'

def get_dbconfigs():
    """ Get DB login config for all databases """

    with open(DATABASE_CONFIG) as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(e)

def load_db(database, schema=None):
    """ 
    Set the database connection to the given DB/schema.
    'database' can be one of 'dssdb', 'search', or 'wagtail'
    """
    
    dbconfigs = get_dbconfigs()
    config_name = '{}_config'.format(database)
    dbconfig = dbconfigs[config_name]
    if not schema:
        pgschema = dbconfigs['pg_schemas'][database]

    return default_scinfo(dbconfig['dbname'], pgschema, dbconfig['host'], dbconfig['user'])

def load_search_db():
    """ Set the database connection to the search DB """
    
    dbconfigs = get_dbconfigs()
    search_dbconfig = dbconfigs['search_config']
    pgschemas = dbconfigs['pg_schemas']

    return default_scinfo(search_dbconfig['dbname'], pgschemas['search'], search_dbconfig['host'], search_dbconfig['user'])

def load_dssdb_db():
    """ Set the database connection to dssdb """
    
    dbconfigs = get_dbconfigs()
    dssdb_dbconfig = dbconfigs['dssdb_config']
    pgschemas = dbconfigs['pg_schemas']

    return default_scinfo(dssdb_dbconfig['dbname'], pgschemas['dssdb'], dssdb_dbconfig['host'], dssdb_dbconfig['user'])

def load_wagtail_db():
    """ Set the database connection to wagtail """
    
    dbconfigs = get_dbconfigs()
    wagtail_dbconfig = dbconfigs['wagtail_config']
    pgschemas = dbconfigs['pg_schemas']

    return default_scinfo(wagtail_dbconfig['dbname'], pgschemas['wagtail'], wagtail_dbconfig['host'], wagtail_dbconfig['user'])