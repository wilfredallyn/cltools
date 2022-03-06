# cltools

Scripts and plugins for c-lightning


### dataexporter.py
* `export_csv`: plugin to export csv files with data about forwards, invoices, pays, and rebalances
* `export_data`: plugin that exports all data

### datalogger.py
* plugin that saves information about `local_failed` forwards by subscribing to `forward_event`

### visualize.py
* `plot_forwards`: plugin that visualizes node data (re-implementation of [manreo's](https://github.com/manreo/lightning-datascience/tree/main/node_info) code)

