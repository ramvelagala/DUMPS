import getpass
import pyodbc
import os
import base64

from blanket.base import BaseClient


class NetezzaClient(BaseClient):
    """
    This class can be used to connect to and query Netezza databases.
    """

    def __init__(self, database=None, servername=None, dsn=None, user_auth=True):
        """

        Initialize connection details for the specified Netezza database or DSN

        Arguments
        ---------
        database : str, optional
            The name of the database ('PBKDMDB'). Optional if specifying the DSN instead
        servername : str, optional
            The name of the server ('prodnzpbkdmdb'). Optional if it can be inferred from the database
        servername : str, optional
            The name of an existing DSN to connect to. Optional if specifying the database details instead
        user_auth : bool
            If True, prompt the user for authentication credentials.
            If False, attempt to get credentials from environment variables.
        """

        if not dsn:
            # If not using a DSN, capture the necessary database/driver details
            self.database = database
            if not servername:
                self.servername = "prodnz{}".format(database.lower())
            else:
                self.servername = servername
            self.port = 5480
            self.driver = self._find_driver('NETEZZA')

        self.dsn = dsn
        self.user_auth = user_auth

    def get_tables(self):
        """ Get Tables

        This method queries Netezza metdata and returns all tables within the
        database this client is connected to.
        """

        query = """
        select
            DISTINCT TABLENAME as "TABLE",
            DATABASE
        from
            _v_table
        order by TABLENAME
        """

        return self.sql(query)

    def get_table_schema(self, table):
        """ Get Table Schema

        This method queries Netezza metdata and returns the column names and
        types within the specified table.

        Arguments
        ---------
        table : str
            The table to return the schema.
        """

        query = """
        select
            COLUMN_NAME as "COLUMN",
            TYPE_NAME as "TYPE",
            TABLE_NAME as "TABLE",
            DATABASE
        from
            _v_sys_columns
        where
            TABLE_NAME = '{}'
        order by
            COLUMN_NAME
        """.format(table)

        return self.sql(query)

    def table_exists(self, table):
        return table in set(self.get_tables().TABLE)

    def _set_connection(self):
        # called by base class

        # create the connection string
        if self.dsn:
            connection_string = "dsn={};".format(self.dsn)
        else :
            connection_string = "driver={};".format(self.driver) + \
                                "servername={};".format(self.servername) + \
                                "port={};".format(self.port) + \
                                "database={};".format(self.database)

        # If using user auth, prompt the user via getpass()
        if (self.user_auth):
            connection_string = connection_string + \
                            "username={};".format(
                                getpass.getpass("Eagle ID: ")
                            ) + \
                            "password={}".format(
                                getpass.getpass("Password: ")
                            )

        # Otherwise, attempt to get credentials from env variables
        else:

            for var in ['DSML_AUTH_USERNAME', 'DSML_AUTH_PASSWORD']:
                if not var in os.environ:
                    raise OSError("Missing required {} environment variable".format(var))

            connection_string = connection_string + \
                            "username={};".format(os.getenv('DSML_AUTH_USERNAME')) + \
                            "password={};".format(
                                base64.b64decode(os.getenv('DSML_AUTH_PASSWORD').encode("utf-8")).decode("utf-8")
                            )

        # Establish connection
        self.connection = pyodbc.connect(connection_string)

    def _find_driver(self, search_term):
        """
        Helper function that searches through list of existing odbc drivers in
        system for an entry that contains the search term.

        Parameters
        ----------
        search_term : str
            Term or pattern to search inside of database driver names.

        Returns
        -------
        driver : str
            String with the name of the database driver to use
        """

        filtered_drivers = list(
            filter(
                lambda x: x.upper().find(search_term.upper()) >= 0,
                pyodbc.drivers()
            )
        )

        # check that at least one option exists
        if len(filtered_drivers) > 0:
            driver = filtered_drivers[0]

            # print a warning that more than one possible driver was found
            if len(filtered_drivers) > 1:
                print("WARNING: Multiple drivers identified.")
                print("Using driver name: " + driver)
            return driver
        else:
            # could not find a driver name to use
            raise EnvironmentError(
                "No ODBC driver " + search_term + "found. " +
                "Please supply a database driver name to use."
            )

    def __contains__(self, table_name):
        return self.table_exists(table_name)

    def __repr__(self):
        return f"{self.__class__.__name__}(database={self.database}, server={self.servername}, dsn={self.dsn})"

    def __str__(self):
        return self.__repr__()
