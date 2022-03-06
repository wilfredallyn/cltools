#!/usr/bin/env python3
from bokeh.io import curdoc
from bokeh.layouts import (
    column,
    row,
    Spacer,
)
from bokeh.models import (
    ColumnDataSource,
    DataTable,
    HoverTool,
    NumberFormatter,
    NumeralTickFormatter,
    Panel,
    TableColumn,
    Tabs,
)
from bokeh.palettes import Set2
from bokeh.plotting import (
    figure,
    output_file,
    save,
)
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
from pyln.client import Plugin
from utils import (
    get_forwards_df,
    get_peers_df,
    get_scid_to_alias_dict,
)


plugin = Plugin()


@plugin.method("plot_forwards")
def plot_forwards(plugin: Plugin):
    """Re-implementation of manreo's node info https://github.com/manreo/lightning-datascience/tree/main/node_info"""
    df_peers = get_peers_df(plugin)

    has_inbound = df_peers["to_us_msat"] > df_peers["our_reserve_msat"]
    inbound_liquidity = (
        df_peers.loc[has_inbound, "to_us_msat"]
        - df_peers.loc[has_inbound, "our_reserve_msat"]
    ).sum()

    has_outbound = (df_peers["total_msat"] - df_peers["to_us_msat"]) > df_peers[
        "their_reserve_msat"
    ]
    outbound_liquidity = (
        df_peers.loc[has_outbound, "total_msat"]
        - df_peers.loc[has_outbound, "to_us_msat"]
        - df_peers.loc[has_outbound, "their_reserve_msat"]
    ).sum()

    # generate monthly plots
    df_forwards = get_forwards_df(plugin)
    df_forwards = df_forwards[df_forwards["status"] == "settled"]

    y_label_dict = {
        "counts": "Num Forwards",
        "fee": "Total Fees (sats)",
        "in_sat": "Total ammount forwarded (sats)",
    }
    tabs_title_dict = {
        "counts": "Forwards counts",
        "fee": "Forwards fees",
        "in_sat": "Forwards amount",
    }

    data_cols = ["counts", "fee", "in_sat"]

    daily_forwards_fig = get_daily_forwards_figure(
        df_forwards, data_cols, y_label_dict, tabs_title_dict
    )
    monthly_forwards_fig = get_monthly_forwards_figure(
        df_forwards, data_cols, y_label_dict, tabs_title_dict
    )

    scid_to_alias_dict = get_scid_to_alias_dict(plugin)
    channel_table = get_channel_forwards_table(df_forwards, scid_to_alias_dict)

    forwards_by_type_fig = get_forwards_by_type_fig(plugin)

    row_space = Spacer(width=100, height=5, sizing_mode="scale_width")
    col_space = Spacer(width=20, height=50)
    left_col = column([channel_table])
    right_col = column(
        [
            daily_forwards_fig,
            row_space,
            monthly_forwards_fig,
            row_space,
            forwards_by_type_fig,
        ]
    )
    final_fig = row([left_col, col_space, right_col])
    output_file("forwards_fig.html")
    save(final_fig)


def get_summary_dfs(df_forwards, groupby_cols):
    df_summary = df_forwards.groupby(groupby_cols).sum()
    df_summary["in_sat"] = df_summary["in_msatoshi"] / 1000
    df_summary["fee"] = df_summary["fee"] / 1000
    df_counts = df_forwards.groupby(groupby_cols).size()
    return df_summary, df_counts


def get_daily_forwards_figure(df_forwards, data_cols, y_label_dict, tabs_title_dict):
    tooltips_dict = {
        "counts": [("total", "@in_sat"), ("fees", "@fee")],
        "fee": [("# Forwards", "@counts"), ("total", "@in_sat")],
        "in_sat": [("# Forwards", "@counts"), ("fees", "@fee")],
    }
    tabs_dict = defaultdict(list)

    groupby_cols = ["year", "month", "day"]
    df_summary, df_counts = get_summary_dfs(df_forwards, groupby_cols)

    for col in data_cols:
        for cur_year in df_summary.index.unique(level="year").sort_values():
            for cur_month in df_summary.index.unique(level="month").sort_values():
                if (cur_year, cur_month) not in df_summary.index:
                    continue
                df_cur = df_summary.loc[cur_year, cur_month].copy()
                df_cur["counts"] = df_counts.loc[cur_year, cur_month]

                p = get_daily_forwards_plot(
                    df=df_cur,
                    x_col="day",
                    y_col=col,
                    tooltips=tooltips_dict[col],
                    x_label=f"{cur_month} - {cur_year}",
                    y_label=y_label_dict[col],
                )

                tabs_dict[col].append(Panel(child=p, title=f"{cur_year}-{cur_month}"))

    forwards_count = Panel(
        child=Tabs(tabs=tabs_dict["counts"]), title=tabs_title_dict["counts"]
    )
    forwards_fees = Panel(
        child=Tabs(tabs=tabs_dict["fee"]), title=tabs_title_dict["fee"]
    )
    forwards_amount = Panel(
        child=Tabs(tabs=tabs_dict["in_sat"]), title=tabs_title_dict["in_sat"]
    )
    daily_forwards_fig = Tabs(tabs=[forwards_count, forwards_fees, forwards_amount])
    return daily_forwards_fig


def get_daily_forwards_plot(df, x_col, y_col, tooltips, x_label, y_label):
    p = figure(width=900, height=300, x_range=(0, 32))
    p.vbar(x=x_col, top=y_col, width=0.9, source=df)
    p.xaxis.axis_label = x_label

    p.add_tools(HoverTool(tooltips=tooltips))
    p.yaxis.axis_label = y_label
    if y_col == "in_sat":
        p.yaxis.formatter = NumeralTickFormatter(format=",")
    return p


def get_monthly_forwards_figure(df_forwards, data_cols, y_label_dict, tabs_title_dict):
    groupby_cols = ["year", "month"]
    df_summary_month, df_counts_month = get_summary_dfs(df_forwards, groupby_cols)
    df_summary_month["counts"] = df_counts_month
    tabs_monthly = []

    for col in data_cols:
        x = [datetime(i[0], i[1], 1) for i in df_summary_month.index]
        y = df_summary_month[col]
        p = figure(width=900, height=300, x_axis_type="datetime")
        p.line(x, y, line_width=2)
        p.yaxis.axis_label = y_label_dict[col]
        if col == "in_sat":
            p.yaxis.formatter = NumeralTickFormatter(format=",")
        tabs_monthly.append(Panel(child=p, title=tabs_title_dict[col]))
    monthly_forwards_fig = Tabs(tabs=tabs_monthly)
    return monthly_forwards_fig


def get_channel_forwards_table(df_forwards, scid_to_alias_dict):
    groupby_cols = ["year", "month", "day"]
    df_summary, df_counts = get_summary_dfs(df_forwards, groupby_cols)
    tabs = []

    for cur_year in df_summary.index.unique(level="year").sort_values():
        for cur_month in df_summary.index.unique(level="month").sort_values():
            if (cur_year, cur_month) not in df_summary.index:
                continue
            data_table = get_forwards_data_table(
                df_forwards, scid_to_alias_dict, cur_year, cur_month
            )
            tabs.append(Panel(child=data_table, title=f"{cur_year}-{cur_month}"))

    # get data for all-time
    data_table = get_forwards_data_table(df_forwards, scid_to_alias_dict)
    tabs.append(Panel(child=data_table, title=f"all-time"))

    channel_table = Tabs(tabs=tabs)
    return channel_table


def get_forwards_data_table(
    df_forwards, scid_to_alias_dict, cur_year=None, cur_month=None
):
    if cur_year and cur_month:
        in_groupby_cols = ["year", "month", "in_channel"]
        out_groupby_cols = ["year", "month", "out_channel"]
        slice_idx = (cur_year, cur_month)
    else:
        in_groupby_cols = ["in_channel"]
        out_groupby_cols = ["out_channel"]
        slice_idx = slice(":")

    source_in = df_forwards.groupby(in_groupby_cols).sum().loc[slice_idx]
    source_out = df_forwards.groupby(out_groupby_cols).sum().loc[slice_idx]
    source_in["counts_in"] = df_forwards.groupby(in_groupby_cols).size().loc[slice_idx]
    source_out["counts_out"] = (
        df_forwards.groupby(out_groupby_cols).size().loc[slice_idx]
    )

    source_in["in_sat"] = round(source_in["in_msatoshi"] / 1000)
    source_out["out_sat"] = round(source_out["out_msatoshi"] / 1000)
    source_in["fee"] = round(source_in["fee"] / 1000)
    source_out["fee"] = round(source_out["fee"] / 1000)

    source_both = pd.merge(
        source_in,
        source_out,
        how="outer",
        left_index=True,
        right_index=True,
        suffixes=["_in", "_out"],
    )
    source_both["total_counts"] = source_both["counts_out"].add(
        source_both["counts_in"], fill_value=0
    )
    source_both = source_both.reset_index().sort_values("total_counts", ascending=False)
    source_both["alias"] = source_both["index"].apply(
        lambda x: scid_to_alias_dict[x] if x in scid_to_alias_dict else "N/A"
    )

    source = ColumnDataSource(source_both.fillna(0))
    columns = [
        TableColumn(field="alias", title="channel"),
        TableColumn(
            field="total_counts",
            title="# Forwards",
            formatter=NumberFormatter(format=","),
        ),
        TableColumn(
            field="in_sat", title="In_amount", formatter=NumberFormatter(format=",")
        ),
        TableColumn(
            field="out_sat", title="Out_amount", formatter=NumberFormatter(format=",")
        ),
        TableColumn(
            field="fee_out", title="Fees", formatter=NumberFormatter(format=",")
        ),
    ]

    data_table = DataTable(source=source, columns=columns, width=600, height=880)
    return data_table


def get_forwards_by_type_fig(plugin: Plugin):
    """Figure of counts by forward type (failed, local_failed, settled, offered)
    https://twitter.com/fiatjaf/status/1495054859984424965
    """
    df_forwards = get_forwards_df(plugin)
    df_counts = df_forwards.groupby(["date", "status"]).size().unstack()
    fwd_types = [x for x in df_counts.columns]

    p = figure(width=900, height=300, x_axis_type="datetime", sizing_mode="scale_both")

    p.vbar_stack(
        fwd_types,
        x="date",
        width=timedelta(days=1),
        line_color="black",
        color=Set2[len(fwd_types)],
        source=df_counts.reset_index(),
        legend_label=fwd_types,
    )
    p.legend.location = "top_right"
    p.legend.title = "Forward Type"
    p.yaxis.axis_label = "Num. of Forwards"
    return p


if __name__ == "__main__":
    plugin.run()
