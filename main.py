"""Data Element Profiler main module."""


from typing import Sequence, TextIO
from grp_bank_dq_engine_dindu.netezza import NetezzaClient

import click
import configparser

client = NetezzaClient()
connection = client.connection()

query_1 = '''SELECT * FROM DBKDMDB.ADMIN.M_CPF_FC_MO_CNSMR_LOAN_ARGT_DTL WHERE "PRODUCT_SEQ_SK" = -1'''
query_2 = '''select column_name from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \
'M_CPF_FC_MO_CNSMR_LOAN_ARGT_DTL' AND COLUMN_NAME LIKE '%_SK' AND DATA_TYPE IN ('INTEGER','SMALLINT','BIGINT') '''





def do_our_task(connection: connection, table: str) -> Sequence[str]:
    """Get all "*SK*" elements of `table` that are an int* type"""
    # query_1 = '''SELECT * FROM DBKDMDB.ADMIN.M_CPF_FC_MO_CNSMR_LOAN_ARGT_DTL WHERE "PRODUCT_SEQ_SK" = -1'''
    query_2 = '''select column_name from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \
    'M_CPF_FC_MO_CNSMR_LOAN_ARGT_DTL' AND COLUMN_NAME LIKE '%_SK' AND DATA_TYPE IN ('INTEGER','SMALLINT','BIGINT') '''

    # query_2 = " select column_name from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}' \
    #     AND COLUMN_NAME = '{1}' AND COLUMN_NAME  LIKE '%_SK' AND DATA_TYPE IN ('INTEGER','SMALLINT','BIGINT') ".format(
    #     'M_CPF_FC_MO_CNSMR_LOAN_ARGT_DTL', "col_name")

    # query_1 = "select * from  DBKDMDB.ADMIN.{0} where '{1}'= -1 }".format(table, '')

    SK_COL_NAMES = connection(query_2)
    print(SK_COL_NAMES)
#     for col_nam in col_names:
#         col_name = connection(query_1)
#         .append()
#     with connection as cursor:
#         return query_for_elements_in_table(cursor, table)
#
#
# @click.command()
# @click.argument('find_sk_ints.txt', type=click.File('r'))
# def main(input: TextIO):
#     """Run from the cli."""
#     client = NetezzaClient()
#     conn = client.connection()
#     config = configparser.ConfigParser()
#
#     with open('./grp_bank_dq_engine_data_element_profiler/venkat.avsc', 'w') as configfile:
#         config.add_section('Tables')
#         for table in input:
#             sk_elements = do_our_task(conn, table)
#             # print(f'{table}: {sk_elements}')
#             config.set('Tables', str(table), str(sk_elements))
#         config.write(configfile)

