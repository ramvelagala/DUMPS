"""Profile the data and generate Various Dataclass objects."""

import pathlib
import json
from typing import List
from collections import namedtuple
from datetime import datetime
import pandas as pd
from grp_bank_dq_engine_data_element_profiler.profiling_classes import ElementNetezza, \
    ElementSnowflake, DomainMetric, FocusMetric, RangeMetric, ValueMetric, ValueMetric2
from grp_bank_dq_engine_data_element_profiler.connection import client_connection
from grp_bank_dq_engine_data_element_profiler._logging import log_file_creation

p_logger = log_file_creation()
SQL_DIR = pathlib.Path(__file__).parent / 'sql'


def meta_data_func(res: object, ctx: object) -> object:
    """Generate Element dataclass feeding meta_data information of every column.

    :param ctx : an object contaning all the command-line inputs given
    :param res: object of dataclass SKColumns.
    :return: List of objects of dataclass Element generated for every column in the
    given table.
    """
    p_logger.debug("inside meta data function")
    element_tuple = namedtuple('Element', 'table column')
    client = client_connection(ctx)
    connection = client.connection
    if ctx.obj["database"] == "Netezza":
        for element in res['elements']:
            element_value = element_tuple(str(res['tablename']), str(element))
            p_logger.debug("column : %s", element_value.column)
            print("count query.")
            count = path_sql("no_of_rows.sql", connection, element_value, ctx)
            print(count)
            print("Query is going to be executed.")
            meta_data = path_sql("main_meta_data.sql", connection, element_value, ctx)
            if (meta_data["IS_NULLABLE"] == "NO").all():
                meta_data["IS_NULLABLE"] = ""
            _, data = next(meta_data.astype(dict(IS_NULLABLE='bool')).iterrows())
            meta_data = {k.casefold(): v for k, v in data.to_dict().items() if
                         v is not None}
            print(meta_data)
            result = ElementNetezza.from_dict(meta_data)
            yield result
    else:
        filename = "snowflake/generate_columns.sql"
        data11 = (connection.sql(SQL_DIR / filename,
                                 dict(table="M_UTL_LK_DEBIT_CARD_TYPE")))
        for num, val in enumerate(data11):
            val = data11.iloc[num]
            meta_data = {k.casefold(): v for k, v in val.to_dict().items()}
            print(meta_data)
            print("Entered here. data_type is dict")
            meta_data['data_type'] = json.loads(meta_data['data_type'])
            for k, v in meta_data['data_type'].items():
                meta_data[k] = v
            meta_data['data_type'] = meta_data["type"]
            meta_data['table'] = meta_data['table_name']
            meta_data['column'] = meta_data['column_name']
            del meta_data["type"]
            del meta_data["table_name"]
            del meta_data['column_name']
            print(meta_data['nullable'])
            print(meta_data)
            result = ElementSnowflake.from_dict(meta_data)
            yield result


def domain_highlights_func(element_list: List[object], ctx: object) -> object:
    """Generate domain metric dataclass.

    Generate DomainMetric dataclass feeding meta_data information of every column
    in the table given by Element Dataclass.

    :param ctx : an object contaning all the command-line inputs given
    :param element_list: list of Element Objects.
    :return: List of objects of dataclass DomainMetric generated for every column in the
    given Element object.
    """
    p_logger.debug("Entered Domain Metrics.")
    client = client_connection(ctx)
    connection = client.connection
    for ele in element_list:
        p_logger.debug("ele: %s", ele)
        if ctx.obj["database"] == "Netezza":
            file_name = "main_domain_highlights.sql"
            print("Entered Netezza path for Domain Metrics.")
            print("Path :", SQL_DIR / file_name)
        elif ctx.obj["database"] == "Snowflake":
            file_name = "snowflake/domain_metrics.sql"
            print("Entered Snow flake path for Domain Metrics.")
            print("Path :", SQL_DIR / file_name)
        data_1 = path_sql(file_name, connection, ele, ctx)
        _, data = next(data_1.fillna(0).iterrows())
        print(data)
        p_logger.debug(("data: %s", data))
        global ROWS  # pylint: disable=global-variable-undefined
        ROWS = data["rows"]
        if ROWS.all() == 0:
            data["Uniqueness"] = 0
        else:
            data["Uniqueness"] = round((data["Cardinality"] * 100 / ROWS), 4)
        data = helper_func(data, ele)
        result = DomainMetric.from_dict(data)
        data = result.to_dict()
        yield data


def range_metric_func(element_list, ctx: object):
    """Generate range metric dataclass.

    Generate Range Metric dataclass feeding meta_data information of every column
    in the table given by Element Dataclass.

    :param ctx : an object contaning all the command-line inputs given
    :param element_list:
    :return: Range Metric Object.
    """
    p_logger.debug("Entered Range Metrics.")
    client = client_connection(ctx)
    connection = client.connection
    for ele in element_list:
        if ctx.obj["database"] == "Netezza":
            file_name = "range_metrics.sql"
            file_name_2 = "std_deviation.sql"
            file_name_str = "range_metrics_str.sql"
            print("Entered Snow flake path for Domain Metrics.")
            print("Path :", SQL_DIR / file_name)
        elif ctx.obj["database"] == "Snowflake":
            file_name = "snowflake/range_metrics.sql"
            file_name_2 = "snowflake/std_deviation.sql"
            file_name_str = "snowflake/range_metrics_str.sql"
            print("Entered Snow flake path for Domain Metrics.")
            print("Path :", SQL_DIR / file_name)
        if any(x in ele.data_type for x in data_types_data()[1]):
            file_name = file_name_str
        if any(x in ele.data_type for x in data_types_data()[2]):
            f_data = {'maximum': 0.0,
                      'mean': 0.0,
                      'median': 0.0,
                      'minimum': 0.0,
                      'standard_deviation': 0.0,
                      'subject': ele
                      }
            print(ele.data_type)
        else:
            data_1 = path_sql(file_name, connection, ele, ctx)
            _, data = next(data_1.fillna(0).iterrows())
            f_data = data.to_dict()
            # try:
            data_1 = path_sql(file_name_2, connection, ele, ctx)
            _, data = next(data_1.fillna(0).iterrows())
            # except:
            #     data['standard_deviation'] = 0
            data['standard_deviation'] = round(data['standard_deviation'], 2)
            f_data.update(data.to_dict())
        data = helper_func(f_data, ele)
        result = RangeMetric.from_dict(data)
        data = result.to_dict()
        yield data


def focus_variable_func(element_list: list, ctx: object):
    """Generate focus metric dataclass.

    Generate Focus variable dataclass having meta_data information of every column
    in the table given by Element Dataclass.If given (list or all) it will iterate and
    generate the required.

    :param ctx : an object containing all the command-line inputs given
    :param element_list: list of Element Objects.
    :return: List of objects
     of dataclass FocusVariable generated for every column in the
    given Element object.
    """
    p_logger.debug("Entered Focus Metrics.")
    focus_values = ctx.obj["focus_values"]
    client = client_connection(ctx)
    connection = client.connection
    for ele in element_list:
        if ctx.obj["database"] == "Netezza":
            file_name = "focus_metrics.sql"
            print("Entered Snow flake path for Domain Metrics.")
            print("Path :", SQL_DIR / file_name)
        elif ctx.obj["database"] == "Snowflake":
            file_name = "snowflake/focus_metrics.sql"
            print("Entered Snow flake path for Domain Metrics.")
            print("Path :", SQL_DIR / file_name)
        list_data = focus_data_type_generation(ctx, ele)
        if len(list_data) == 0:
            data = pd.DataFrame(columns=["focus_value"])
        else:
            if len(list_data) == 1:
                ctx.obj['focus_where'] = "= '{list_data}'". \
                    format(list_data=list_data[0])
            else:
                ctx.obj['focus_where'] = tuple_creation(list_data)
            data = path_sql("focus_metrics.sql", connection, ele, ctx)
        data_dd = data["focus_value"].reset_index(drop=True)
        list_dd = []
        for num in data_dd:
            list_dd.append(str(num))
        for num in focus_values:
            if num not in list_dd:
                new_row = {"focus_value": num, "count": 0}
                data = data.append(new_row, ignore_index=True)
        list1 = [round((val * 100 / ROWS), 4) for val in data["count"]]
        data.insert(0, 'coverage', list1)
        list2 = data.to_dict(orient="records")
        data = []
        for val in list2:
            val.update({"subject": ele.to_dict()})
            result = FocusMetric.from_dict(val)
            data.append(result.to_dict())
        yield list2


def value_metric_func(element_list: List[object], need: str, ctx: object) -> list:
    """Generate Value Metric Dataclass elements.

    Generate list of value metric profiling data for every column in the table.
    :param element_list: list containing all the column name and thier meta data in the
     table.
    :param need: a string value of either TOP/BOT to profile for lower or top values.
    :param ctx: object containing all the command line arguments.
    :return: list containing value metric profiling data.
    """
    p_logger.debug("Entered Value Metrics.")
    p_logger.debug("need: %s", need)
    p_logger.debug("limit: %d", ctx.obj["limit"])
    p_logger.debug("date: %s", ctx.obj["date_start"])
    need = str(need).lower()

    val_count = "{0}_val_count".format(need)
    coverage = "{0}_coverage".format(need)
    client = client_connection(ctx)
    connection = client.connection
    for ele in element_list:
        if ctx.obj["database"] == "Netezza":
            file_name = "value_metrics_{0}.sql".format(need)
            print("Entered Snow flake path for Domain Metrics.")
            print("Path :", SQL_DIR / file_name)
        elif ctx.obj["database"] == "Snowflake":
            file_name = "snowflake/value_metrics_{0}.sql".format(need)
            print("Entered Snow flake path for Domain Metrics.")
            print("Path :", SQL_DIR / file_name)
        data_1 = path_sql(file_name, connection, ele, ctx)
        data = data_1.fillna("Null Values")
        if 'top_value' in data.columns:
            if data.dtypes['top_value'] == object and type(data['top_value'][0]) == datetime:
                data['top_value'] = pd.to_datetime(data['top_value'], errors='coerce')
        if 'bot_value' in data.columns:
            if data.dtypes['bot_value'] == object and type(data['bot_value'][0]) == datetime:
                data['bot_value'] = pd.to_datetime(data['bot_value'], errors='coerce')
        list1 = [round((val * 100 / ROWS), 4) for val in data[val_count]]
        data.insert(0, coverage, list1)
        record_list = data.to_dict(orient="records")
        data = []
        for val in record_list:
            val.update({"subject": ele.to_dict()})
            if need == "top":
                result = ValueMetric.from_dict(val)
            else:
                result = ValueMetric2.from_dict(val)
            data.append(result.to_dict())
        yield data
    connection.close()


def random_data_func(element_list: list, ctx: object):
    with client_connection(ctx) as connection:
        for ele in element_list:
            data = path_sql("random_data.sql", connection, ele, ctx)
            print(data)
            data_dd = data[ele.column].tolist()
            print(data_dd)
            yield data_dd


def helper_func(data, ele: object) -> dict:
    """Create lower case values.

    function to modify keys from upper case to lower case and ignore values which has
    None as values.
    :param data:
    :param ele:
    :return: a dictionary of the passed data after above modification.
    """
    if not isinstance(data, dict):
        data = data.to_dict()
    data = {k.casefold(): v for k, v in data.items() if v is not None}
    data.update({"subject": ele.to_dict()})
    return data


def path_sql(sqlfile: str, connection: object, ele: object, ctx: object) ->pd.DataFrame:
    """Generate profiling data by hitting the nettezza/snowflake with passed parameters.

    :param sqlfile: name of the sql file.
    :param connection: netezza jdbc connection object.
    :param ele: object of element class
    :param ctx: object containing all the command line arguments.
    :return: pandas data frame which has profiling data.
    """
    print("table", ele.table)
    print("Table_type", type(ele.table))
    print("column", ele.column)
    print("limit", ctx.obj['limit'])
    print("focuswhere", ctx.obj['focus_where'])
    print("date_con", ctx.obj["date_con"])
    print(ctx.obj)
    print(SQL_DIR / sqlfile)
    data = (connection.sql(SQL_DIR / sqlfile,
                           dict(table=ele.table, column=ele.column,
                                limit=ctx.obj['limit'],
                                focuswhere=ctx.obj['focus_where'],
                                date_con=ctx.obj["date_con"],
                                where_con=ctx.obj["where_con"])))

    print("data", data)
    return data


def tuple_creation(value: tuple) -> str:
    """Generate a string which has function values.

    create a string containing focus variables given a tuple of focus variables, making
    it passable into sql.
    :param value:
    :return: a string containing focus variables in the format( in (0,1)).
    """
    focus_where = "in {0}".format(value)
    return focus_where


def json_generation(table_list: list):
    """Append json to file in required format.

    :param table_list: contains the value for all the profiling data.
    :return:
    """
    with open("grp_bank_dq_engine_data_element_profiler/html/version_1_result.json",
              "w") as out_file:
        json.dump({"result": table_list}, out_file)


def data_types_data():

    numeric_data_types = ['NUMBER', 'INT', 'BIGINT', 'SMALLINT', 'INTEGER', 'FLOAT',
                          'FLOAT4', 'FLOAT8', 'DECIMAL', 'NUMERIC', 'DOUBLE', 'REAL',
                          'DOUBLE PRECISION']
    str_data_types = ['VARCHAR', 'CHAR', 'CHARACTER', 'STRING', 'TEXT', 'BINARY',
                      'VARBINARY']
    date_data_types = ['DATE', 'DATETIME', 'TIME', 'TIMESTAMP', 'TIMESTAMP_LTZ',
                       'TIMESTAMP_NTZ', 'TIMESTAMP_TZ']
    semi_structured_data_types = ['VARIANT', 'OBJECT', 'ARRAY']
    bool_data_types = ['BOOLEAN']

    return [numeric_data_types, str_data_types, date_data_types,
            semi_structured_data_types, bool_data_types]

def focus_data_type_generation(ctx: object, ele: object) -> tuple:
    """Generate a list of integers in the focus values tuple.

    :param ctx: object containing all the command line arguments.
    :return: tuple of all int numbers.
    """
    focus_values = ctx.obj["focus_values"]
    list_data = []

    for val in focus_values:
        try:
            element_datatype = str(ele.data_type).lower()
            # if "char" in element_datatype:
            if any(x in element_datatype for x in  data_types_data()[1]):
                val = str(val)
                list_data.append(val)
            # data_type = numeric_data_types
            if any(x in element_datatype for x in data_types_data()[0]):
                val = float(val)
                list_data.append(val)
            # data_type = date_data_types
            if any(x in element_datatype for x in data_types_data()[2]):
                from datetime import datetime
                val = datetime.strptime(val, "%Y-%m-%d")
                print(val)
                list_data.append(val)
            else:
                pass
        except ValueError:
            pass
        except TypeError:
            pass
    return tuple(list_data)


def schema_generation(ctx: object, column: str) -> tuple:
    """Generate schema by splitting the table, schema, data store names.

    :param ctx: object containing all the command line arguments.
    :param thing: string which contains schema,database and table, columns.
    :return: ctx object and table names.
    """
    ctx.obj["datastore"] = column.split(".")[0]
    ctx.obj["database"] = column.split(".")[1]
    if ctx.obj["database"] == "ADMIN":
        ctx.obj["database"] = "Netezza"
    elif ctx.obj["database"] == "BNK":
        ctx.obj["database"] = "Snowflake"
    table = column.split(".")[2]
    print("table:", table)
    return ctx, table
