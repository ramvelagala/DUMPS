"""Data Element Profiler main module"""

import configparser
import logging
from typing import Sequence, TextIO, Tuple
import click
from grp_bank_dq_engine_dindu.netezza import NetezzaClient
from grp_bank_dq_engine_data_element_profiler.profiling_classes import SkColumns
from grp_bank_dq_engine_data_element_profiler.profiling_values import SQL_DIR, \
    meta_data_func, domain_highlights_func, focus_variable_func, Range_metric_func,value_metric_func


logger = logging.getLogger(__name__)
logger.debug("debug logging initialized")

config = configparser.ConfigParser()
config.read('database_logins.ini')


def do_our_task(connection, table: str) -> Sequence[Tuple[str, str]]:
    """Get all "*SK*" elements of `table` that are an int* type.

    yeild {columnname, datatype}
    """
    logger.debug('running query...')

    for _ in connection.sql(SQL_DIR / "_sk_column_name_type_int.sql",
                            dict(table=table)).itertuples():
        yield tuple(_)[1:]


@click.command()
@click.argument('input_file', type=click.File('r'))
def main(input_file: TextIO):
    """

    :param input_file: dbdetails.txt
    :return: Focusvariable, DomainMetric, Element, RangeMetric classes objects for every
     column in the table that satisfy the criteria.
    """

    logger.debug(f'recieved {input_file}')
    # setup task from input
    args = dict(config[input_file.name])
    # connect to database
    client = NetezzaClient(**args)
    logger.debug(f'{client}')

    # print(SkColumns.json_schema())

    with client.connection as connection:

        logger.debug('connected to db')
        for num, table in enumerate(input_file):
            table = table.rpartition('.')[-1].strip()
            logger.debug(f'processing {table}')
            sk_elements = do_our_task(connection, table)

            res = SkColumns(tablename=table,
                            elements=[[element_name, element_type, nullable] for
                                      element_name, element_type, nullable in
                                      sk_elements])
            value = res.to_dict()
            print(f"{num}: {res.to_json()}")
            meta_data_func(value)
            meta_list = list(meta_data_func(value))
            print("This is meta List.")
            print(meta_list)
            domain_list = list(domain_highlights_func(meta_list))
            print("This is domain_list.")
            print(domain_list)
            # metric_list = list(focus_variable_func(meta_list))
            # print("This is metric_list.")
            # print(metric_list)
            # range_metric_list = list(Range_metric_func(meta_list))
            # print("This is Range Metric List.")
            # print(range_metric_list)
            # focus_variable_list = list(focus_variable_func(meta_list))
            # print("This is focus Metric List.")
            # print(focus_variable_list)
            # valuemetric_1_list = list(value_metric_func(meta_list, "Top"))
            # print("This is some metric_list.")
            # print(valuemetric_1_list)
            if num >= 0:
                break


main()
