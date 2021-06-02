import pandas as pd
import os
import six
import string


class BaseClient(object):
    """
    This base class can be used to connect to and query a variety of database backends.
    """
    
    def __init__(self, **args):
        self.connection = None

    def sql(self, query, template=None):
        """
        Executes a SQL SELECT query against an opened connection, returning
        results in a Pandas DataFrame.

        Arguments
        ---------
        query : str, file
            A query string, path-string, or readable file object that is passed 
            through the connection to query. If a string, the string is checked
            for the existence of any files with that name. If the file exists,
            then the file is loaded. 
        template : dict
            A dictionary which defines the terms to find and replace in the sql
            query string. The dictionary keys define the terms to find, and the
            dictionary values the corresponding replacement. This method uses
            the `string.Template` method and as such expect search terms to be
            identified by `$`.

        Returns
        -------
        df : dataframe
            A Pandas DataFrame containing the results of the query.

        Examples
        --------
        .. code-block:: python

            from dsml.clients import NetezzaClient
            nz_client = NetezzaClient('PBKDMDB').open()

            query = 'select distinct $COLUMN_NAME from $TABLE_NAME'
            template = {
                'COLUMN_NAME': 'USAA_PARTY_SK',
                'TABLE_NAME': 'M_BFT_FC_CD_IRA_DEP_MLY'
            }

            # Method 1
            df = nz_client.sql(query, template)
            
            # Method 2
            df = nz_client.sql("/path/to/query.sql", template)
            
            # Method 3
            df = nz_client.sql(open("/path/to/query.sql", "r"), template)
        """

        query = self.preprocess_query(query, template)

        return pd.read_sql(query, self.connection)


    def execute(self, query, template=None):
        """
        Executes a statement (e.g., a non-SELECT SQL query) against an opened connection.

        Arguments
        ---------
        query : str, file
            A query string or readable file object that is passed through the
            odbc connection to query.
        template : dict
            A dictionary which defines the terms to find and replace in the sql
            query string. The dictionary keys define the terms to find, and the
            dictionary values the corresponding replacement. This method uses
            the `string.Template` method and as such expect search terms to be
            identified by `$`.

        Examples
        --------
        .. code-block:: python

            from dsml.clients import HiveClient
            hive_client = HiveClient(clustername='disc', database='dz_1234_disc').open()

            query = 'drop table if exists my_temp_table'
            hive_client.execute(query)
        """

        query = self.preprocess_query(query, template)

        pd.io.sql.execute(query, self.connection)

    def open(self):
        # Open connection
        self._set_connection()

        if not hasattr(self, 'connection') or self.connection is None:
            raise NotImplementedError("child class implementation of _set_connection"
                "should set self.connection")

        return self

    def reauth(self):
        # Re-Authenticate the connection. This allows the user to reconnect to the source
        # using the same configuration as the original connection. Depending on how
        # the authentication was done originally, credentials may be requested.
        self.close()

        self._set_connection()

    def close(self):
        # close the database connection
        if hasattr(self, 'connection') and self.connection is not None:
            self.connection.close()
            self.connection = None

    def __enter__(self):
        """ Enter `with` method

        This function defines the second half of the code required to enable
        this class to work via with statements in python. This code will open
        the connection to the database and store it within the object.

        Returns
        -------
        self
            Returns a reference to the BaseClient object
        """
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Exit `with` method

        This function defines the second half of the code required to enable
        this class to work via with statements in python. It will ensure that
        the database connection is properly closed.

        Arguments
        ---------
        exc_type :
            type of exception that occured (if any)
        exc_val :
            value of exception that occured (if any)
        exc_tb :
            exception traceback that occured (if any)
        """
        self.close()


    def preprocess_query(self, query, template=None):
        """
        Helper function that preprocesses a query (and optional template) for execution

        Parameters
        ----------
        query : str, file
        template : dict

        Returns
        -------
        query : str
            A query string ready for execution
        """

        if isinstance(query, six.string_types):
            file_path = os.path.expanduser(query)
            if os.path.isfile(file_path):
                with open(file_path, 'r') as query_file:
                    query = query_file.read()
        else:
            # Try to read as bytes object if not a string
            try:
                query = query.read()
            except:
                raise ValueError(
                    "Unable to interpret query. Expected str or bytes object."
                )

        if template is not None:
            query = string.Template(query).safe_substitute(**template)

        return query
    
    def _set_connection(self):
        raise NotImplementedError("_set_connection must be implemented by child"
            "classes which implement BaseClient")
