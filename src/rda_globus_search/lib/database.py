import os
import yaml
import json
import pathlib
import sqlite3
import typing as t

from rda_python_common.PgDBI import default_scinfo

DATABASE_CONFIG = '/glade/u/home/rdadata/.pgconfig.yml'
SQLITE_STORAGE = '/glade/u/home/rdadata/globus/.globus_search.db'

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

class SQLiteAdapter:
    """
    :param dbname: The name of the DB file to write to and read from.
    :param namespace: A "namespace" to use within the database. All operations will
        be performed indexed under this string, so that multiple distinct sets of info
        may be stored in the database.
    :param connect_params: A pass-through dictionary for fine-tuning the SQLite
         connection.

    The ``connect_params`` is an optional dictionary whose elements are passed directly
    to the underlying ``sqlite3.connect()`` method, enabling developers to fine-tune the
    connection to the SQLite database.  Refer to the ``sqlite3.connect()``
    documentation for SQLite-specific parameters.

    SQL command to create the config table used here.  This will need to be 
    called externally if the desired table doesn't exist:
        CREATE TABLE config_storage (
                namespace VARCHAR NOT NULL,
                config_name VARCHAR NOT NULL,
                config_data_json VARCHAR NOT NULL,
                PRIMARY KEY (namespace, config_name)
                );
    """

    def __init__(
        self,
        dbname: pathlib.Path | str,
        *,
        namespace: str = "DEFAULT",
        connect_params: dict[str, t.Any] | None = None,
    ) -> None:
        self.filename = self.dbname = str(dbname)
        self.namespace = namespace
        self._connection = self._init_and_connect(connect_params)

    def _init_and_connect(
        self,
        connect_params: dict[str, t.Any] | None,
    ) -> sqlite3.Connection:
        if not os.path.exists(self.filename):
            raise FileNotFoundError("Database {} does not exist.".format(self.filename))
        connect_params = connect_params or {}
        conn = sqlite3.connect(self.dbname, **connect_params)
        return conn

    def close(self) -> None:
        """
        Close the underlying database connection.
        """
        self._connection.close()

    def store_config(
        self, config_name: str, config_dict: t.Mapping[str, t.Any]
    ) -> None:
        """
        :param config_name: A string name for the configuration value
        :param config_dict: A dict of config which will be stored serialized as JSON

        Store a config dict under the current namespace in the config table.
        Allows arbitrary configuration data to be namespaced under the namespace, so
        that application config may be associated with the stored tokens.

        Uses sqlite "REPLACE" to perform the operation.
        """
        self._connection.execute(
            "REPLACE INTO config_storage(namespace, config_name, config_data_json) "
            "VALUES (?, ?, ?)",
            (self.namespace, config_name, json.dumps(config_dict)),
        )
        self._connection.commit()

    def read_config(self, config_name: str) -> dict[str, t.Any] | None:
        """
        :param config_name: A string name for the configuration value

        Load a config dict under the current namespace in the config table.
        If no value is found, returns None
        """
        row = self._connection.execute(
            "SELECT config_data_json FROM config_storage "
            "WHERE namespace=? AND config_name=?",
            (self.namespace, config_name),
        ).fetchone()

        if row is None:
            return None
        config_data_json = row[0]
        val = json.loads(config_data_json)
        if not isinstance(val, dict):
            raise ValueError("reading config data and got non-dict result")
        return val

    def remove_config(self, config_name: str) -> bool:
        """
        :param config_name: A string name for the configuration value

        Delete a previously stored configuration value.

        Returns True if data was deleted, False if none was found to delete.
        """
        rowcount = self._connection.execute(
            "DELETE FROM config_storage WHERE namespace=? AND config_name=?",
            (self.namespace, config_name),
        ).rowcount
        self._connection.commit()
        return rowcount != 0

def config_storage_adapter():
    if not hasattr(config_storage_adapter, "_instance"):
        # namespace is equal to the current environment
        config_storage_adapter._instance = SQLiteAdapter(SQLITE_STORAGE, namespace="DEFAULT")
    return config_storage_adapter._instance
