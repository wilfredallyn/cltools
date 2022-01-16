#!/usr/bin/env python3
from datetime import datetime
from pyln.client import Plugin


plugin = Plugin()


@plugin.subscribe("forward_event")
def handle_failed_foward(plugin, forward_event, **kwargs):
    if forward_event["status"] == "local_failed":
        forward_event["received_timestamp"] = datetime.utcfromtimestamp(
            forward_event["received_time"]
        ).strftime("%Y-%m-%d %H:%M:%S")
        with open("./local_failed_forward_log.txt", "a") as f:
            print(forward_event, file=f)


plugin.run()
