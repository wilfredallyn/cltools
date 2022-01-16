import pprint
import pandas as pd


# node
def get_node_alias(plugin, node_id: str) -> str:
    """Takes node id, return node alias"""
    return plugin.rpc.listnodes(node_id)["nodes"][0]["alias"]


# peers
def get_active_peers(plugin) -> list:
    """Return list of all peers with active channel"""
    peers_list = [
        peer
        for peer in plugin.rpc.listpeers()["peers"]
        if peer["channels"] and peer["channels"][0]["state"] == "CHANNELD_NORMAL"
    ]
    return peers_list


def get_peer_scid_dict(plugin) -> dict:
    """Return dict that maps peer node id to short channel id"""
    peers_list = get_active_peers(plugin)
    scid_dict = {}
    for peer in peers_list:
        scid_dict[peer["id"]] = peer["channels"][0]["short_channel_id"]
    return scid_dict


def get_peers_df(plugin) -> pd.DataFrame:
    """Return dataframe with peers info

    Dataframe has node_id index and columns of scid and alias
    """
    scid_dict = get_peer_scid_dict(plugin)
    alias_dict = {}

    for node_id in scid_dict:
        alias_dict[node_id] = get_node_alias(plugin, node_id)

    df_peers = pd.DataFrame(
        {
            "scid": pd.Series(scid_dict),
            "alias": pd.Series(alias_dict),
        }
    )
    df_peers.index.name = "id"
    return df_peers


# balance
def get_local_balance(plugin) -> int:
    """Return int of total local balance"""
    peers_list = get_active_peers(plugin)
    local_balance = sum([peer["channels"][0]["msatoshi_to_us"] for peer in peers_list])
    return local_balance


# invoices
def get_rebalances(plugin) -> list:
    """Return list of all rebalances (paid invoices that start with Rebalance label"""
    invoices = plugin.rpc.listinvoices()["invoices"]
    rebalances = [
        i
        for i in invoices
        if i.get("status") == "paid" and i.get("label").startswith("Rebalance")
    ]
    return rebalances


# pays
def get_pays_dict(plugin) -> dict:
    """Return dict of pays with payment_hash key"""
    pays = plugin.rpc.listpays()["pays"]
    pays_dict = {p["payment_hash"]: p for p in pays if p.get("status") == "complete"}
    return pays_dict
