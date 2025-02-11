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

    Valid database names: 'dssdb', 'search', or 'wagtail'.
    Schema name will default to the database name unless
    otherwise specified.
    """
    
    dbconfigs = get_dbconfigs()
    config_name = '{}_config'.format(database)
    dbconfig = dbconfigs[config_name]

    if not schema:
        pgschema = dbconfigs['pg_schemas'][database]

    return default_scinfo(dbconfig['dbname'], pgschema, dbconfig['host'], dbconfig['user'])
