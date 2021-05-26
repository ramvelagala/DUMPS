"""Testing meta data connections sql and schema."""
# pylint: disable=unused-import
# pylint: disable=import-error
# pylint: disable=redefined-outer-name

import pytest
from tests.test_connection import netezza_connection
from grp_bank_dq_engine_data_element_profiler.profiling_values import SQL_DIR, ProfilingValues




@pytest.fixture()
def whatever_metadata(netezza_connection):
    """Fixture to return data for domain metric.

    :param netezza_connection:
    :return: Data frame which has domain metric values.
    """
    with netezza_connection.connection as connection:
        _, meta_data = next(connection.sql(SQL_DIR / "main_meta_data.sql",
                                           dict(table="PROFILER_TESTING",
                                                column="COL_INT",
                                                date_con="COL_DATE = 2021-03-15")).
                            astype(dict(IS_NULLABLE='bool')).iterrows())
        return meta_data


pf = ProfilingValues()
obj = dict()
obj["database"] = "Netezza"
values = dict()
values['PROFILER_TESTING'] = ['COL_INT']
dd=pf.meta_data_func(values, obj)

@pytest.mark.xfail(raises=ValueError)
def test_domain_metric_value_sql(dd):
    """Testing whether sql is passing and raise value error if error occurs.

    :return: assert result is not None.
    """
    with pytest.raises(Exception):
        print("Query error has not been raised")
        whatever_metadata()
