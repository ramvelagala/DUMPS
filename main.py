"""Profile the data and generate Various Dataclass objects."""

import configparser
import pathlib
from typing import List
from grp_bank_dq_engine_dindu.netezza import NetezzaClient
from grp_bank_dq_engine_data_element_profiler.profiling_classes import Element, \
    DomainMetric, FocusMetric, RangeMetric, ValueMetric, ValueMetric2
import pdb

SQL_DIR = pathlib.Path(__file__).parent / 'sql'
config = configparser.ConfigParser()
config.read('database_logins.ini')
args = dict(config['dbdetails.txt'])
client = NetezzaClient(**args)


def meta_data_func(res: object) -> object:
    """Generate Element dataclass feeding meta_data information of every column
    in the table.

    :param res: object of dataclass SKColumns.
    :return: List of objects of dataclass Element generated for every column in the
    given table.
    """
    table = str(res['tablename'])
    for element in res['elements']:
        column = str(element[0])
        # print(column)
        with client.connection as connection:
            _, meta_data = next(connection.sql(SQL_DIR / "main_meta_data.sql",
                                               dict(table=table, column=column)).
                                astype(dict(IS_NULLABLE='bool')).iterrows())
            meta_data = {k.casefold(): v for k, v in meta_data.to_dict().items() if
                         v is not None}
            result = Element.from_dict(meta_data)
            yield result
            break


# working.
def domain_highlights_func(element_list: List[object]) -> object:
    """Generate DomainMetric dataclass feeding meta_data information of every column
    in the table given by Element Dataclass.

    :param element_list: list of Element Objects.
    :return: List of objects of dataclass DomainMetric generated for every column in the
    given Element object.
    """
    with client.connection as connection:
            for ele in element_list:
                _, data = next(connection.sql(SQL_DIR / "main_domain_highlights.sql",
                                                 dict(table=ele.table,
                                                      column=ele.column)).iterrows())
                global rows
                rows = data["rows"]
                data["Uniqueness"] = round((data["Cardinality"] * 100 / rows), 4)
                dh_data = {k.casefold(): v for k, v in data.to_dict().items() if
                           v is not None}
                print("it's ok")
                print(dh_data)
                dh_data.update({"subject": ele.to_dict()})
                result = DomainMetric.from_dict(dh_data)
                # yield dh_data
                yield result
                break
    # print("dfsdf")
    d = path123("main_domain_highlights.sql", element_list, check="domains")
    # print(d)
    # return d


# working.
def Range_metric_func(element_list):
    """Test the domain_metric connections.

    :param element_list:
    :return: Range Metric Object.
    """
    with client.connection as connection:
        for ele in element_list:
            _, data = next(connection.sql(SQL_DIR / 'Range_Metric' / "range_metric.sql",
                                  dict(table=ele.table, column=ele.column)).fillna(0).iterrows())
            f_data = data.to_dict()
            _, data = next(connection.sql(SQL_DIR / 'Range_Metric' / "std_deviation.sql",
                                  dict(table=ele.table, column=ele.column)).fillna(0).iterrows())
            data['standard_deviation'] = round(data['standard_deviation'], 2)
            f_data.update(data.to_dict())
            data = {k.casefold(): v for k, v in f_data.items() if
                    v is not None}
            data.update({"subject": ele.to_dict()})
            result = RangeMetric.from_dict(data)
            yield result


def focus_variable_func(element_list, value='-1'):
    """Generate Focusvariable dataclass having meta_data information of every column
        in the table given by Element Dataclass.
        If given (list or all) it will iterate and generate the required.

    :param element_list: list of Element Objects.
    :param value: focus variable_value.
    :return: List of objects of dataclass FocusVariable generated for every column in the
    given Element object.
    """

    focus_where = "in (-1,null)"
    with client.connection as connection:
        for ele in element_list:
            _, data = next(connection.sql(SQL_DIR / "focus_variable.sql",
                                          dict(table=ele.table,
                                               column=ele.column,
                                               focuswhere=focus_where)).iterrows())

            data = {k.casefold(): v for k, v in data.to_dict().items() if
                    v is not None}
            data.update({"subject": ele.to_dict()})
            print("This is data.")
            print(data)
            result = FocusMetric.from_dict(data)
            yield result


def value_metric_func(element_list: List[object], need):
    with client.connection as connection:
        for ele in element_list:
            limit = 5
            rows = 15000
            if need == "Top":
                filename = "top_25_distinct.sql"
                print("Top")
            else :
                filename = "bot_25_distinct.sql"
                print("Bot")
            data = (connection.sql(SQL_DIR / "top_25_distinct.sql",
                                  dict(table=ele.table, column=ele.column,
                                       limit="5", rows="6091"))).fillna(0)
            print(data)
            list = [round((val * 100 / rows), 4) for val in data["top_val_count"]]
            data.insert(0, 'top_coverage', list)
            data.update({"subject": ele.to_dict()})
            list = data.to_dict(orient="records")
            d = []
            for val in list:
                if need == "Top":
                    result = ValueMetric.from_dict(val)
                else:
                    result = ValueMetric2.from_dict(val)

                d.append(result.to_dict())
            yield d


def path123(sqlfile, element_list, check):
    print(sqlfile, element_list, type(element_list), "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    with client.connection as connection:
        for ele in element_list:
            _, data = next(connection.sql(SQL_DIR / sqlfile,
                             dict(table=ele.table,  column=ele.column)).iterrows())
            #TODO helper function.
            dh_data = {k.casefold(): v for k, v in data.to_dict().items() if
                       v is not None}
            # dh_data.update({"subject": ele.to_dict()})
            print(dh_data)
            if check == "domains":
                result = DomainMetric.from_dict(dh_data)
                print("domains", result)
            return result
