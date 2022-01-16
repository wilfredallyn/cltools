import time
import pytest
from pyln.testing.fixtures import (
    node_factory,
    bitcoind,
)


@pytest.fixture
def network(request, node_factory, bitcoind):
    # pass plugin options as parameter to test with this code:
    #   @pytest.mark.parametrize("network", [[plugin_opt, rebalance_plugin_path]], indirect=True)
    plugin_opt, rebalance_plugin_path = request.param

    # https://github.com/lightningd/plugins/blob/2560a9a85f6ca0224e8a4ce1aa75c74b93d68f2b/rebalance/test_rebalance.py#L35
    l1, l2, l3 = node_factory.line_graph(3, opts=plugin_opt)
    l1.rpc.plugin_start(rebalance_plugin_path)
    nodes = [l1, l2, l3]

    # form a circle so we can do rebalancing
    l3.connect(l1)
    l3.fundchannel(l1)

    # get scids
    scid12 = l1.get_channel_scid(l2)
    scid23 = l2.get_channel_scid(l3)
    scid31 = l3.get_channel_scid(l1)
    scids = [scid12, scid23, scid31]

    # wait for each others gossip
    bitcoind.generate_block(6)
    for n in nodes:
        for scid in scids:
            try:
                n.wait_channel_active(scid)
            except ValueError:  # sleep if wait_channel_active times out
                time.sleep(10)

    l1.rpc.rebalance(scid12, scid31)

    if len(l1.rpc.listinvoices()["invoices"]) == 0:
        # retry failed rebalance
        time.sleep(10)
        l1.rpc.rebalance(scid12, scid31)

    return {
        "l1": l1,
        "l2": l2,
        "l3": l3,
    }
