import configparser
# from typing import List
from grp_bank_dq_engine_dindu.netezza import NetezzaClient
from grp_bank_dq_engine_data_element_profiler.profiling_classes import Element, \
    DomainMetric, FocusMetric, RangeMetric, ValueMetric, ValueMetric2
from grp_bank_dq_engine_data_element_profiler.profiling_values import SQL_DIR
import pandas as pd


config = configparser.ConfigParser()
config.read('database_logins.ini')
args = dict(config['Netezza.txt'])
client = NetezzaClient(**args)

# Insert Values. ----- working
with client.connection as connection:
    meta_data = connection.execute(SQL_DIR / "netezza_temp_con.sql")
    # dat = pd.read_excel('version_1_mock_data.xlsx', usecols='A, B, C')
    # print(dat, type(dat))
    # for num, row in enumerate(dat.itertuples()):
    #     # print(row)
    #     # print(row.int_val, row.str_val, row.date_val)
    #     # print(type(row.int_val), type(row.str_val), type(row.date_val))
    #     # if row.int_val:
    #         # row.
    #         # connection.execute("INSERT INTO TESTING_1 (column_int, column_str,"
    #         #                    " column_date) VALUES (2021, 'Ram', '2021-4-15')")
    #
    #         # print(row._1, row.Charlotte, type(row._3))
    #     date = str(row.date_val).split(' ')[0]
    #     # print(type(date), date)
    #     # print(row.str_val, type(row.str_val))
    #     # dd = connection.execute("Show tables;")
    #     # print(dd, "%%%%%%%%%%%%%")
    #
    #     try:
    #
    #         connection.execute("INSERT INTO VERSION_TEST_OK_PLEASE (column_int, column_str,"
    #                            " column_date) VALUES ({0}, '{1}', '{2}')".format(row.int_val,
    #                                                                          str(
    #                                                                              row.str_val),
    #                                                                          date))
    #         print(row)
    #     except Exception as e:
    #         print(e)
    #         print("Data Inserted........")
    #
    #         #print(row._1, row.Charlotte, row._3)
    #         # break
    #
    #
    #     if num > 100:
    #         print("Data Inserted 100 records .")
    #         break
    #

quit()
with client.connection as connection:
    meta_data = connection.sql(SQL_DIR / "main_meta_data.sql",
                                       dict(table="PROFILER_SK_NEW" ,
                                            column="VALUE_SK"))
    if (meta_data["IS_NULLABLE"] == "NO").all():
        meta_data["IS_NULLABLE"] = ""
    _, data = next(meta_data.astype(dict(IS_NULLABLE='bool')).iterrows())
    meta_data = {k.casefold(): v for k, v in data.to_dict().items() if
                 v is not None}
    result = Element.from_dict(meta_data)


def domain_metric_json():
    with client.connection as connection:
        _, data = next(connection.sql(SQL_DIR / "main_domain_highlights.sql",
                                      dict(table="Profiler_sk",
                                           column="value_sk")).iterrows())
        global rows
        rows = data["rows"]
        data["Uniqueness"] = round((data["Cardinality"] * 100 / rows), 4)
        data = {k.casefold(): v for k, v in data.to_dict().items() if
                v is not None}
        result = DomainMetric.from_dict(data)
        print(result.to_json())
        return result.to_dict()


data1 = domain_metric_json()
result_dict_list.append(data1)


print(type(data1))


with client.connection as connection:
    meta_data =connection.sql(SQL_DIR / "main_meta_data.sql",
                                       dict(table="Profiler_sk", column="value_sk"))
    # meta_data = {k.casefold(): v for k, v in meta_data.to_dict().items() if
    #              v is not None}
    # result = Element.from_dict(meta_data)
    print(meta_data, type(meta_data))


working
def range_metric_json():
    with client.connection as connection:

        _, data = next(connection.sql(SQL_DIR / 'Range_Metric' / "range_metric.sql",
                                      dict(table="Profiler_sk",
                                           column="value_sk")).iterrows())
        f_data = data.to_dict()
        _, data = next(connection.sql(SQL_DIR / 'Range_Metric' / "std_deviation.sql",
                                      dict(table="Profiler_sk",
                                           column="value_sk")).iterrows())
        data['standard_deviation'] = round(data['standard_deviation'], 2)
        f_data.update(data.to_dict())
        data = {k.casefold(): v for k, v in f_data.items() if
                v is not None}
        result = RangeMetric.from_dict(data)

        # print(type(result), "FDSFDD")
        return result.to_dict()


data2 = range_metric_json()
result_dict_list.append(data2)

print(SQL_DIR)
def value_metric():
    with client.connection as connection:
        # table.rows from domain_high
        limit = 5
        rows = 15000
        data = connection.sql(SQL_DIR / "top_25_distinct.sql",
                              dict(table="Profiler_sk", column="value_sk",
                                   limit="5", rows="6091"))
        list = [round((val * 100 / rows), 4) for val in data["top_val_count"]]
        data.insert(0, 'top_coverage', list)
        list = data.to_dict(orient="records")
        d = []
        for val in list:
            result = ValueMetric.from_dict(val)
            d.append(result.to_dict())
        return d


data3 = value_metric()
data3 = {'value_metric': data3}
result_dict_list.append(data3)

#
def value_metric_2():
    with client.connection as connection:
        # table.rows from domain_high
        limit = 5
        rows = 15000
        data = connection.sql(SQL_DIR / "bot_25_distinct.sql",
                              dict(table="Profiler_sk", column="value_sk",
                                   limit="5", rows="6091"))
        list_1 = [round((val * 100 / rows), 4) for val in data["bot_val_count"]]
        data.insert(0, 'bot_coverage', list_1)
        list_1 = data.to_dict(orient="records")
        d = []
        for val in list_1:
            result = ValueMetric2.from_dict(val)
            d.append(result.to_dict())
        return d


data5 = value_metric_2()
data5 = {'value_metric_2': data5}
result_dict_list.append(data5)


def focus_variable():
    with client.connection as connection:
        focus_where = "in (28, 22, 30, -1)"
        rows =15000
        data = connection.sql(SQL_DIR / "focus_variable.sql",
                              dict(table="Profiler_sk", column="value_sk",
                                   focuswhere=focus_where, rows=rows))
        list = [round((val * 100 / rows), 4) for val in data["count"]]
        data.insert(0, 'coverage', list)
        list = data.to_dict(orient="records")
        d = []
        for val in list:
            print(val)
            result = FocusMetric.from_dict(val)
            print(result)
            print(result.to_json())
            d.append(result.to_dict())
        return d


data4 = focus_variable()
data4 = {"focus_metric": data4}
result_dict_list.append(data4)

with open("Demo/result.json", "w") as out_file:
    json.dump({"result": result_dict_list}, out_file)
