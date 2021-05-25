"""Data Element Profiler main module."""

from datetime import date, timedelta
import click
import json
from grp_bank_dq_engine_data_element_profiler.connection import client_connection
from grp_bank_dq_engine_data_element_profiler.profiling_classes import SkColumns
from grp_bank_dq_engine_data_element_profiler.profiling_values import meta_data_func, \
    domain_highlights_func, focus_variable_func, range_metric_func, \
    value_metric_func, json_generation, schema_generation, random_data_func
from grp_bank_dq_engine_data_element_profiler._logging import log_file_creation
from grp_bank_dq_engine_data_element_profiler.generate_columns import column_gen
import datetime

p_logger = log_file_creation()


@click.group(invoke_without_command=True)
@click.option("--focus_values", '-fv', multiple=True, default=['-1', '!'])
@click.option("--date-start", '-ds', type=click.DateTime(formats=["%Y-%m-%d"]),
              default="2021-03-01")
@click.option("--date-end", '-de', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(date.today() + timedelta(days=1)))
@click.option("--limit", '-l', type=int, default=5)
@click.option("--flag_domain", '-df', type=bool, default=True)
@click.option("--flag_range", '-rf', type=bool, default=True)
@click.option("--flag_value", '-vf', type=bool, default=True)
@click.option("--flag_focus", '-ff', type=bool, default=True)
@click.option("--where_con", '-wc', type=str, default="")
@click.pass_context
def main(ctx, focus_values, limit, date_start, date_end, flag_domain, flag_range,
         flag_value, flag_focus, where_con):
    """

    Take a file with list of columns and generate list of element objects.

    :return: List of element objects
    """
    print("Called Main")
    ctx.obj["focus_values"] = focus_values
    ctx.obj["focus_where"] = ""
    ctx.obj["limit"] = limit
    ctx.obj["date_start"] = date_start
    ctx.obj["flag_domain"] = flag_domain
    ctx.obj["flag_range"] = flag_range
    ctx.obj["flag_value"] = flag_value
    ctx.obj["flag_focus"] = flag_focus
    ctx.obj["where_con"] = where_con
    # date_start = str(date_start).split(" ")[0]
    # date_end = str(date_end).split(" ")[0]
    if date_end == str(date.today() + timedelta(days=1)):
        date_con = f"AS_OF_DT = '{date_start}'"
        ctx.obj["date_con"] = date_con
    else:
        date_con = f"AS_OF_DT BETWEEN '{date_start}' and '{date_end}'"
        ctx.obj["date_con"] = date_con
    p_logger.debug("date condition : %s", ctx.obj["date_con"])


@click.command()
@click.option("--input_file", '-i')
@click.pass_context
def file_input(ctx, input_file):
    """Command line argument for taking a file as input.

    Take a file as CL input and generate all element objects having table with
    corresponding column names.

    :return: FocusMetric, DomainMetric, Element, RangeMetric classes objects for every
     column in the table that satisfy the criteria.
    """
    with open(input_file) as data:
        data = json.load(data)
    p_logger.debug(f"data: {data}")
    table_list = []
    for num, (key, val) in enumerate(data.items()):
        table = key
        columns = val.get("column")
        ctx.obj["focus_where"] = ""
        for k in val.keys():
            ctx.obj[k] = val.get(k)
        if ctx.obj["date_start"] == "":
            ctx.obj["date_start"] = str(date.today() - timedelta(days=1))
        if val.get("date_end") == "":
            date_con = f"AS_OF_DT = '{datetime.datetime.strptime(val.get('date_start'), '%Y-%m-%d')}'"
            ctx.obj["date_con"] = date_con
        else:
            date_con = f"AS_OF_DT BETWEEN '{datetime.datetime.strptime(val.get('date_start'), '%Y-%m-%d')}' and '{datetime.datetime.strptime(val.get('date_end'), '%Y-%m-%d')}'"
            ctx.obj["date_con"] = date_con

        print(
            f"focus_values: {ctx.obj['focus_values']}, limit: {ctx.obj['limit']}, date_start: {ctx.obj['date_start']}, domain_flag :{ctx.obj['flag_domain']}, range_flag: {ctx.obj['flag_range']}, value_flag: {ctx.obj['flag_value']}, focus_flag: {ctx.obj['flag_focus']}")
        ctx_1, table = schema_generation(ctx, table)
        client = client_connection(ctx_1)
        connection = client.connection
        if len(columns) == 0:
            columns = column_gen(connection, table)
        res = SkColumns(tablename=table,
                        elements=[element_name for element_name in columns])
        value = res.to_dict()
        p_logger.debug("%s: %s", num, res.to_json())
        p_logger.debug("focus_values: %s, limit: %s, date_start: %s",
                       ctx_1.obj['focus_values'], ctx_1.obj['limit'],
                       ctx_1.obj['date_start'])
        everything(value, ctx_1, table_list)
        connection.close()
    json_generation(table_list)


@click.command()
@click.option("--table_name", '-t', multiple=True, type=str)
@click.pass_context
def table_input(ctx, table_name):
    """Command line argument for taking a table name/s as input.

    Take a table name as CL input and generate all column
    names.

    :return: element object.
    """
    click.echo(f"table names:{table_name}")
    table_list = []
    for num, table in enumerate(table_name):
        ctx_1, table = schema_generation(ctx, table)
        client = client_connection(ctx)
        connection = client.connection
        p_logger.debug('connected to db')
        p_logger.debug("processing %s", table)
        columns = column_gen(connection, table, ctx_1)
        print("columns generated.")
        print(columns)
        res = SkColumns(tablename=table,
                        elements=[element_name for element_name in columns])
        value = res.to_dict()
        print("LIne 135.")
        print("value: ", value)
        p_logger.debug("%s: %s", num, res.to_json())
        p_logger.debug("focus_values: %s, limit: %s, date_start: %s",
                       ctx_1.obj['focus_values'], ctx_1.obj['limit'],
                       ctx_1.obj['date_start'])
        # connection.close()
        print("connection closed.")
        everything(value, ctx_1, table_list)

    json_generation(table_list)


@click.command()
@click.option("--coltab", '-ct', multiple=True)
@click.pass_context
def column_input(ctx, coltab):
    """Command line argument for taking a column name/s as input.

    Take a list of column  names and corresponding table names as input and generate
    element Object.
    pass tuples (table,column). this accomadates multiple table thingies.

    :return: element object.
    """
    click.echo(coltab)
    click.echo(type(coltab))
    table_list = []
    for num, column in enumerate(coltab):
        ctx_1, table = schema_generation(ctx, column)
        columns = table.split(",")[1:]
        table = table.split(",")[0]
        p_logger.debug("val:%s", column)
        p_logger.debug('connected to db')
        res = SkColumns(tablename=table,
                        elements=[element_name for element_name in columns])
        value = res.to_dict()
        p_logger.debug("%s: %s", num, res.to_json())
        p_logger.debug("database: %s,focus_values: %s, limit: %s, date_start: %s",
                       ctx_1.obj['database'], ctx_1.obj['focus_values'],
                       ctx_1.obj['limit'], ctx_1.obj['date_start'])
        everything(value, ctx_1, table_list)
    json_generation(table_list)


def everything(value, ctx, table_list):
    """Generate json having all the profiling data.

    calls all the data-profiling functions to generate final output.
    :param value: a dictionary with tables as key and respective column as values.
    :param ctx: a object which contains all the user input values from command line.
    :param table_list: list containing values with corresponding table name as key.
    :return:
    """
    meta_list = list(meta_data_func(value, ctx))
    meta_data_1 = [i.to_dict() for i in meta_list]
    p_logger.debug("metalist: %s", str(meta_list))
    if ctx.obj["flag_domain"]:
        domain_metric_list = list(domain_highlights_func(meta_list, ctx))
        p_logger.debug(domain_metric_list[0])
    else:
        domain_metric_list = []
    if ctx.obj["flag_range"]:
        range_metric_list = list(range_metric_func(meta_list, ctx))
        p_logger.debug(range_metric_list[0])
    else:
        range_metric_list = []
    if ctx.obj["flag_value"]:
        valuemetric_1_list = list(value_metric_func(meta_list, "top", ctx))
        p_logger.debug(valuemetric_1_list[0])
        valuemetric_2_list = list(value_metric_func(meta_list, "bot", ctx))
        p_logger.debug(valuemetric_2_list[0])
    else:
        valuemetric_1_list = []
        valuemetric_2_list = []
    if ctx.obj["flag_focus"]:
        focus_variable_list = list(focus_variable_func(meta_list, ctx))
        p_logger.debug(focus_variable_list[0])
    else:
        focus_variable_list = []
    quit()
    if ctx.obj["random_data"]:
        random_data_list = list(random_data_func(meta_list, ctx))
        p_logger.debug(random_data_list)
    else:
        focus_variable_list = []
    col_list = []
    table_name = meta_data_1[0]['table']
    col_data_list = []
    for val in meta_data_1:
        list_result1 = [{'meta_data': val},
                        {'domain_metric': value for value in domain_metric_list if
                         value['subject'] == val},
                        {'range_metric': value for value in range_metric_list if
                         value['subject'] == val},
                        {'val_metric_1': value for value in valuemetric_1_list if
                         value[0]['subject'] == val},
                        {'val_metric_2': value for value in valuemetric_2_list if
                         value[0]['subject'] == val},
                        {'focus_metric': value for value in focus_variable_list if
                         value[0]['subject'] == val}]
        print(list_result1)
        col_data_list.append({val['column']: list_result1})
    col_list.append({'COLUMNS': col_data_list})
    col_list.append({'data_store': ctx.obj['datastore']})
    col_list.append({'database': ctx.obj['database']})
    table_list.append({table_name: col_list})


main.add_command(table_input)
main.add_command(column_input)
main.add_command(file_input)

if __name__ == "__main__":
    main(obj={})  # pylint: disable=no-value-for-parameter,unexpected-keyword-arg
