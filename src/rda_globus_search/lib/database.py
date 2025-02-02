import yaml
from rda_python_common.PgDBI import default_scinfo

def get_dbconfigs():
    """ Get DB login config for all databases """

    with open('/glade/u/home/rdadata/.pgconfig.yml') as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(e)

def load_search_db():
    """ Set the database connection to the search DB """
    
    dbconfigs = get_dbconfigs()
    search_dbconfig = dbconfigs['search_config']
    pgschemas = dbconfigs['pg_schemas']

    return default_scinfo(search_dbconfig['dbname'], pgschemas['search'], search_dbconfig['host'], search_dbconfig['user'])