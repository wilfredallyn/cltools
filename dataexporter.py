#!/usr/bin/env python3
from datetime import datetime
import os
import pandas as pd
from pyln.client import Plugin
import utils


plugin = Plugin()


def get_rebalances_df(plugin: Plugin) -> pd.DataFrame:
    """Returns dataframe with rebalances data"""
    pays_dict = utils.get_pays_dict(plugin)
    df = pd.DataFrame(utils.get_rebalances(plugin))
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

        df_peers = utils.get_peers_df(plugin)
        scid_to_alias_dict = df_peers.set_index("scid").to_dict()["alias"]

        df["out_alias"] = df["out_channel"].apply(lambda x: scid_to_alias_dict.get(x))
        df["in_alias"] = df["in_channel"].apply(lambda x: scid_to_alias_dict.get(x))
    return df


def get_forwards_df(plugin: Plugin) -> pd.DataFrame:
    """Returns dataframe with output from 'lightning-cli listforwards'"""
    df = pd.DataFrame(plugin.rpc.listforwards()["forwards"])

    if len(df) > 0:  # has at least 1 forward
        df_peers = utils.get_peers_df(plugin)
        scid_to_alias_dict = df_peers.set_index("scid").to_dict()["alias"]

        df["out_alias"] = df["out_channel"].apply(lambda x: scid_to_alias_dict.get(x))
        df["in_alias"] = df["in_channel"].apply(lambda x: scid_to_alias_dict.get(x))

        df["received_timestamp"] = df["received_time"].apply(
            lambda x: datetime.utcfromtimestamp(x)
        )
    return df


def get_pays_df(plugin: Plugin) -> pd.DataFrame:
    """Returns dataframe with output from 'lightning-cli listpays'"""
    df = pd.DataFrame(plugin.rpc.listpays()["pays"])
    if len(df) > 0:  # has at least 1 pay
        df["destination_alias"] = df["destination"].apply(
            lambda x: utils.get_node_alias(plugin, x)
        )
        df["created_at_timestamp"] = df["created_at"].apply(
            lambda x: datetime.utcfromtimestamp(x)
        )
        null_idx = df["amount_msat"].isnull()
        df["fees"] = (
            df.loc[~null_idx, "amount_sent_msat"] - df.loc[~null_idx, "amount_msat"]
        )
    return df


def get_invoices_df(plugin: Plugin) -> pd.DataFrame:
    """Returns dataframe with output from 'lightning-cli listinvoices'"""
    df = pd.DataFrame(plugin.rpc.listinvoices()["invoices"])
    if len(df) > 0:  # has at least 1 invoice
        null_idx = df["paid_at"].isnull()
        df["paid_at_timestamp"] = df.loc[~null_idx, "paid_at"].apply(
            lambda x: datetime.utcfromtimestamp(x)
        )
    return df


def write_csv(df: pd.DataFrame, csv_file: str) -> None:
    """Write dataframe to csv file"""
    df.to_csv(csv_file, index=False)


@plugin.method("export_csv")
def export_csv(plugin: Plugin, data_type: str, csv_file: str) -> None:
    """Export data to csv file"""
    if data_type == "rebalances":
        df = get_rebalances_df(plugin)
    elif data_type == "forwards":
        df = get_forwards_df(plugin)
    elif data_type == "pays":
        df = get_pays_df(plugin)
    elif data_type == "invoices":
        df = get_invoices_df(plugin)
    else:
        raise ValueError(f"data_type {data_type} not recognized")
    write_csv(df, csv_file)


@plugin.method("exportdata")
def exportdata(plugin: Plugin, output_dir: str = os.getcwd()) -> None:
    """Export all data to csv files in output directory"""
    data_type_list = [
        "rebalances",
        "forwards",
        "pays",
        "invoices",
    ]
    for data_type in data_type_list:
        csv_file = os.path.join(output_dir, f"{data_type}.csv")
        export_csv(plugin, data_type, csv_file)


if __name__ == "__main__":
    plugin.run()
