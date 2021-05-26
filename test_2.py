"""Testing connection value metric connections sql and schema."""
# pylint: disable=unused-import
# pylint: disable=import-error
# pylint: disable=redefined-outer-name

import pytest
from tests.test_connection import netezza_connection
from tests.test_meta_data import whatever_metadata
from grp_bank_dq_engine_data_element_profiler.profiling_values import SQL_DIR
from grp_bank_dq_engine_data_element_profiler.profiling_classes import \
    ElementNetezza, ElementSnowflake,ValueMetric, ValueMetric2


@pytest.fixture()
def whatever_value(netezza_connection, need):
    """Fixture to return data for value metric.

    :param netezza_connection: a jdbc client to connect to database
    :param need: top or bot
    :return: Data frame which has value metric values.
    """
    file_name = "value_metrics_{0}.sql".format(need)
    with netezza_connection.connection as connection:
        query = connection.sql(SQL_DIR / file_name,
                               dict(table="PROFILER_TESTING",
                                    column="COL_INT", limit=5,
                                    date_con="COL_DATE = '2021-03-15'",
                                    where_con=""))
        return query


@pytest.mark.xfail(raises=ValueError)
@pytest.mark.parametrize("need", ['top', 'bot'])
def test_value_metric_value_sql(whatever_value):
    """Test value metric values passing.

    :param whatever_value:
    :return: checks whether any exception rose.
    """
    with pytest.raises(Exception):
        print("Query error has not been raised")
        whatever_value()


@pytest.fixture()
def value_metric_schema(need, whatever_metadata, whatever_value):
    """Fixture which returns values of value_metric.

    :return:List of data value metric elements.
    """
    val_count = "{0}_val_count".format(need)
    coverage = "{0}_coverage".format(need)
    meta_data = whatever_metadata
    data = whatever_value.fillna(0)
    list1 = [round((val * 100 / 150000), 4) for val in data[val_count]]
    data.insert(0, coverage, list1)
    record_list = data.to_dict(orient="records")
    data_list = []
    meta_data = {k.casefold(): v for k, v in meta_data.to_dict().items() if
                 v is not None}
    ele = ElementNetezza.from_dict(meta_data)
    for val in record_list:
        val.update({"subject": ele.to_dict()})
        if need == "top":
            result = ValueMetric.from_dict(val)
        else:
            result = ValueMetric2.from_dict(val)
        data_list.append(result.to_dict())
    yield data_list


@pytest.mark.parametrize("need", ['top', 'bot'])
def test_value_metric_schema(value_metric_schema):
    """Testing value_metric schema whether it matches schema or not.

    :return:assert whether schema matches value metric schema.
    """
    value = value_metric_schema
    assert value is not None

