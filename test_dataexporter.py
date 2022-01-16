import pandas as pd
import pytest
import time
from pyln.testing.fixtures import *
from dataexporter import (
    get_forwards_df,
    get_invoices_df,
    get_pays_df,
    get_rebalances_df,
)


plugin_path = os.path.join(os.path.dirname(__file__), "dataexporter.py")
plugin_opt = {"plugin": plugin_path}

# specify path to rebalance plugin here
rebalance_plugin_path = (
    "/home/bitcoin/cl-plugins-available/plugins/rebalance/rebalance.py"
)


# use network fixture from conftest.py
@pytest.mark.parametrize(
    "network", [[plugin_opt, rebalance_plugin_path]], indirect=True
)
def test_dataexporter(network, check):
    # all tests in single method since node_factory, bitcoind are function-scoped fixtures
    l1 = network["l1"]

    # Check that dataframe functions work
    check.is_instance(get_forwards_df(l1), pd.DataFrame, "get_forwards_df failed")
    check.is_instance(get_invoices_df(l1), pd.DataFrame, "get_invoices_df failed")
    check.is_instance(get_pays_df(l1), pd.DataFrame, "get_pays_df failed")
    check.is_instance(get_rebalances_df(l1), pd.DataFrame, "get_rebalances_df failed")

    l1.rpc.exportdata()

    data_types_list = [
        "forwards",
        "invoices",
        "pays",
        "rebalances",
    ]

    # Check that data exported to csv files
    for data_type in data_types_list:
        csv_file = os.path.join(
            l1.daemon.opts["bitcoin-datadir"], "regtest", f"{data_type}.csv"
        )
        check.is_true(os.path.isfile(csv_file), f"{csv_file} export failed")
