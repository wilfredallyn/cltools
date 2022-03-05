from collections import defaultdict
from datetime import datetime
import pprint
import pandas as pd
from pyln.client import Plugin


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


def get_peers_df(plugin) -> pd.DataFrame:
    """Return dataframe with peers info"""
    df = pd.DataFrame(get_active_peers(plugin))
    df_channels = df.pop("channels")
    df = pd.concat([df, df_channels.apply(lambda x: pd.Series(x[0]))], axis=1)
    df["alias"] = df["id"].apply(lambda x: get_node_alias(plugin, x))
    df = df.set_index("id")
    return df


def get_scid_to_alias_dict(plugin):
    df_peers = get_peers_df(plugin)
    scid_to_alias_dict = df_peers.set_index("short_channel_id").to_dict()["alias"]
    return scid_to_alias_dict


# balance
def get_local_balance(plugin) -> int:
    """Return int of total local balance"""
    peers_list = get_active_peers(plugin)
    local_balance = sum([peer["channels"][0]["msatoshi_to_us"] for peer in peers_list])
    return local_balance


# invoices
def get_invoices_df(plugin: Plugin) -> pd.DataFrame:
    """Returns dataframe with output from 'lightning-cli listinvoices'"""
    df = pd.DataFrame(plugin.rpc.listinvoices()["invoices"])
    if len(df) > 0:  # has at least 1 invoice
        null_idx = df["paid_at"].isnull()
        df["paid_at_timestamp"] = df.loc[~null_idx, "paid_at"].apply(
            lambda x: datetime.utcfromtimestamp(x)
        )
    return df


def get_rebalances(plugin) -> list:
    """Return list of all rebalances (paid invoices that start with Rebalance label"""
    invoices = plugin.rpc.listinvoices()["invoices"]
    rebalances = [
        i
        for i in invoices
        if i.get("status") == "paid" and i.get("label").startswith("Rebalance")
    ]
    return rebalances


def get_rebalances_df(plugin: Plugin) -> pd.DataFrame:
    """Returns dataframe with rebalances data"""
    pays_dict = get_pays_dict(plugin)
    df = pd.DataFrame(get_rebalances(plugin))
    if len(df) > 0:  # has at least 1 rebalance
        df["amount_sent_msat"] = df["payment_hash"].apply(
            lambda x: pays_dict[x]["amount_sent_msat"]
        )
        df["fees"] = df["amount_sent_msat"] - df["amount_msat"]
        df["fee_rate_ppm"] = df["fees"] / df["amount_msat"] * 1000000
        df["paid_at_timestamp"] = df["paid_at"].apply(
            lambda x: datetime.utcfromtimestamp(x)
        )  # .strftime('%Y-%m-%d %H:%M:%S')
        df[["out_channel", "in_channel"]] = df["description"].str.split(
            " to ", expand=True
        )

        scid_to_alias_dict = get_scid_to_alias_dict(plugin)
        df["out_alias"] = df["out_channel"].apply(lambda x: scid_to_alias_dict.get(x))
        df["in_alias"] = df["in_channel"].apply(lambda x: scid_to_alias_dict.get(x))
    return df


def get_forwards_df(plugin: Plugin) -> pd.DataFrame:
    """Returns dataframe with output from 'lightning-cli listforwards'"""
    df = pd.DataFrame(plugin.rpc.listforwards()["forwards"])

    if len(df) > 0:  # has at least 1 forward
        scid_to_alias_dict = get_scid_to_alias_dict(plugin)
        df["out_alias"] = df["out_channel"].apply(lambda x: scid_to_alias_dict.get(x))
        df["in_alias"] = df["in_channel"].apply(lambda x: scid_to_alias_dict.get(x))

        df["received_timestamp"] = df["received_time"].apply(
            lambda x: datetime.utcfromtimestamp(x)
        )
        df["year"] = df["received_timestamp"].dt.year
        df["month"] = df["received_timestamp"].dt.month
        df["day"] = df["received_timestamp"].dt.day
        df["date"] = df["received_timestamp"].dt.date
    return df


# pays
def get_pays_dict(plugin) -> dict:
    """Return dict of pays with payment_hash key"""
    pays = plugin.rpc.listpays()["pays"]
    pays_dict = {p["payment_hash"]: p for p in pays if p.get("status") == "complete"}
    return pays_dict


def get_pays_df(plugin: Plugin) -> pd.DataFrame:
    """Returns dataframe with output from 'lightning-cli listpays'"""
    df = pd.DataFrame(plugin.rpc.listpays()["pays"])
    if len(df) > 0:  # has at least 1 pay
        df["destination_alias"] = df["destination"].apply(
            lambda x: get_node_alias(plugin, x)
        )
        df["created_at_timestamp"] = df["created_at"].apply(
            lambda x: datetime.utcfromtimestamp(x)
        )
        null_idx = df["amount_msat"].isnull()
        df["fees"] = (
            df.loc[~null_idx, "amount_sent_msat"] - df.loc[~null_idx, "amount_msat"]
        )
    return df
