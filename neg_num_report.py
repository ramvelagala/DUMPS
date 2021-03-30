import configparser
import pytest
from grp_bank_dq_engine_data_element_profiler.profiling_classes import Element, \
    DomainMetric, RangeMetric
from grp_bank_dq_engine_dindu.netezza import NetezzaClient
from grp_bank_dq_engine_data_element_profiler.profiling_values import SQL_DIR

table = "M_BFT_FC_TRN_FIX_TERM_MNTY"
column = "TRN_FIX_TRM_MTY_SEQ_SK"


@pytest.fixture
def netezza_connection():
    config = configparser.ConfigParser()
    config.read('database_logins.ini')
    args = dict(config['dbdetails.txt'])
    yield NetezzaClient(**args)


def test_domain_metric_connection(netezza_connection):
    """Test the domain_metric connections.

    :param netezza_connection:
    :return: Range Metric Object.
    """
    client = netezza_connection
    number = 75
    limit = 10
    with client.connection as connection:
        data = connection.sql(SQL_DIR / 'Range_Metric' / "highest_freq_value.sql",
                              dict(table=table, column=column))

        f_data = {"highest_freq_value": data.to_dict()}
        data = connection.sql(SQL_DIR / 'Range_Metric' / "lowest_freq_value.sql",
                              dict(table=table, column=column))
        f_data.update({"lowest_freq_value": data.to_dict()})
        data = connection.sql(SQL_DIR / 'Range_Metric' / "range_metric.sql",
                              dict(table=table, column=column))
        f_data.update(data.to_dict())
        data = connection.sql(SQL_DIR / 'Range_Metric' / "median.sql",
                              dict(table=table, column=column))
        f_data.update(data.to_dict())
        data = connection.sql(SQL_DIR / 'Range_Metric' / "mode.sql",
                              dict(table=table, column=column))
        f_data.update(data.to_dict())
        data = connection.sql(SQL_DIR / 'Range_Metric' / "std_deviation.sql",
                              dict(table=table, column=column))
        f_data.update(data.to_dict())
        data = {k.casefold(): v for k, v in f_data.items() if
                     v is not None}

        print(data)


def test_meta_data_connection(netezza_connection):
    client = netezza_connection
    table = "M_BFT_FC_TRN_FIX_TERM_MNTY"
    column = "TRN_FIX_TRM_MTY_SEQ_SK"
    with client.connection as connection:
        _, meta_data = next(connection.sql(SQL_DIR / "main_meta_data.sql",
                                           dict(table=table, column=column)).astype(
            dict(IS_NULLABLE='bool')).iterrows())
        meta_data = {k.casefold(): v for k, v in meta_data.to_dict().items() if
                     v is not None}
        result = Element.from_dict(meta_data)
        print(result)
        print(result.table_name,result.column_name)


def test_domain_highlights_connection(netezza_connection):
    client = netezza_connection
    with client.connection as connection:
        _, meta_data = next(connection.sql(SQL_DIR / "main_meta_data.sql",
                                           dict(table=table, column=column)).astype(
            dict(IS_NULLABLE='bool')).iterrows())
        meta_data = {k.casefold(): v for k, v in meta_data.to_dict().items() if
                     v is not None}

    with client.connection as connection:
        _, dh_data = next(connection.sql(SQL_DIR / "main_domain_highlights.sql",
                                         dict(table=table, column=column)).iterrows())

        dh_data = {k.casefold(): v for k, v in dh_data.to_dict().items() if
                   v is not None}
        dh_data.update(meta_data)
        result = DomainMetric.from_dict(dh_data)
        print(result)
