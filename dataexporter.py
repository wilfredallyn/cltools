#!/usr/bin/env python3
import os
import pandas as pd
from pyln.client import Plugin
import utils


plugin = Plugin()


def write_csv(df: pd.DataFrame, csv_file: str) -> None:
    """Write dataframe to csv file"""
    df.to_csv(csv_file)


@plugin.method("export_csv")
def export_csv(plugin: Plugin, data_type: str, csv_file: str) -> None:
    """Export data to csv file"""
    if data_type == "rebalances":
        df = utils.get_rebalances_df(plugin)
    elif data_type == "forwards":
        df = utils.get_forwards_df(plugin)
    elif data_type == "pays":
        df = utils.get_pays_df(plugin)
    elif data_type == "invoices":
        df = utils.get_invoices_df(plugin)
    elif data_type == "peers":
        df = utils.get_peers_df(plugin)
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
        "peers",
    ]
    for data_type in data_type_list:
        csv_file = os.path.join(output_dir, f"{data_type}.csv")
        export_csv(plugin, data_type, csv_file)


if __name__ == "__main__":
    plugin.run()
