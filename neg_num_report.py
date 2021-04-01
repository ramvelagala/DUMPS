from tests.test_connection import netezza_connection
from grp_bank_dq_engine_data_element_profiler.profiling_values import SQL_DIR, helper_func
from grp_bank_dq_engine_data_element_profiler.profiling_classes import DomainMetric,Element
import pytest

@pytest.fixture()
def whatever(netezza_connection):
    with netezza_connection.connection as connection:
        query = connection.sql(SQL_DIR / "main_domain_highlights.sql",
                               dict(table="M_BFT_FC_TRN_FIX_TERM_MNTY",
                                    column="TRN_FIX_TRM_MTY_SEQ_K"))
        return query

@pytest.mark.xfail(raises=ValueError)
def test_domain_metric_value_sql(whatever):
    with pytest.raises(Exception):
        print("Value error has been raised")
        whatever()


def test_domain_metric_schema(netezza_connection):
    with netezza_connection.connection as connection:
        _,data = next(connection.sql(SQL_DIR / "main_domain_highlights.sql",
                             dict(table="M_BFT_FC_TRN_FIX_TERM_MNTY",
                                column="TRN_FIX_TRM_MTY_SEQ_SK")).fillna(0).iterrows())
        _, meta_data = next(connection.sql(SQL_DIR / "main_meta_data.sql",
                                           dict(table="M_BFT_FC_TRN_FIX_TERM_MNTY",
                                                column="TRN_FIX_TRM_MTY_SEQ_SK")).
                            astype(dict(IS_NULLABLE='bool')).iterrows())
        meta_data = {k.casefold(): v for k, v in meta_data.to_dict().items() if
                     v is not None}
        ele = Element.from_dict(meta_data)
        print(data)
        rows = data["rows"]
        if rows == 0:
            data["Uniqueness"] = 0
        else:
            data["Uniqueness"] = round((data["Cardinality"] * 100 / rows), 4)
        data = helper_func(data, ele)
        result = DomainMetric.from_dict(data)
        print(result)
        assert result is not None
