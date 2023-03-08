import requests
import os
import logging
import re

logging.basicConfig(level=logging.INFO)

api_key = os.getenv("DD_API_KEY")
app_key = os.getenv("DD_APP_KEY")

if not (api_key := os.getenv("DD_API_KEY")):
    raise ValueError("DD_API_KEY not found in environment, please ensure it is set")

if not (app_key := os.getenv("DD_APP_KEY")):
    raise ValueError("DD_APP_KEY not found in environment, please ensure it is set")

HEADERS = {"DD-API-KEY": api_key, "DD-APPLICATION-KEY": app_key}

METRIC_TAG_REGEX = r"{(.*?)}"
QUERY_TAG_REGEX = r"(service:\w+)"

DASHBOARDS = {}
MONITORS = {}
NOTEBOOKS = {}
SLOS = {}
DEDUPED_DASHBOARDS = {}
DEDUPED_MONITORS = {}
DEDUPED_NOTEBOOKS = {}
DEDUPED_SLOS = {}


def api_request(path):
    try:
        url = f"https://api.datadoghq.com/{path}"
        resp = requests.get(url, headers=HEADERS)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        logging.error(
            f"Failed making request to {url}, request status {resp.status_code}"
        )
        raise (e)
    except requests.JSONDecodeError:
        logging.error(f"Datadog API did not return valid JSON")
        raise (e)


#### Resource collection


def get_all_dashboard_ids():
    dashboards = []
    resp = api_request(f"api/v1/dashboard")
    for dashboard in resp["dashboards"]:
        dashboards.append(dashboard["id"])
    return dashboards


def get_dashboard_requests(dash_id):
    dashboard = api_request(f"api/v1/dashboard/{dash_id}")
    try:
        for widget in dashboard["widgets"]:
            if widget["definition"].get("type", "") == "group":
                for group_widget in widget["definition"]["widgets"]:
                    widget_dispatch(group_widget, dash_id)
            else:
                widget_dispatch(widget, dash_id)

    except Exception as e:
        logging.warning(e)


def get_all_monitors():
    monitors = api_request("api/v1/monitor")
    for monitor in monitors:
        monitor_type_dispatch(monitor, monitor["id"])


def get_all_slos():
    slos = api_request("api/v1/slo")
    for slo in slos["data"]:
        if slo["type"] == "metric":
            search_query_tags(slo["query"]["denominator"], slo["id"], SLOS)
            search_query_tags(slo["query"]["numerator"], slo["id"], SLOS)


def get_all_notebooks():
    notebooks = api_request("api/v1/notebooks")
    for notebook in notebooks["data"]:
        for cell in notebook["attributes"]["cells"]:
            for request in cell["attributes"]["definition"].get("requests", []):
                if "queries" in request:
                    for query in request["queries"]:
                        if query["data_source"] == "metrics":
                            query_dispatch(
                                {
                                    "data_source": query["data_source"],
                                    "query": query["query"],
                                },
                                notebook["id"],
                                notebook=True,
                            )
                        elif query["data_source"] in [
                            "logs",
                            "spans",
                            "rum",
                            "security_signals",
                            "profiles",
                            "network",
                            "network_device_flows",
                            "events",
                            "process",
                            "incident_analytics",
                            "apm_sec_spans",
                            "app_sec_spans",
                            "database_queries",
                            "synthetics_batches",
                        ]:
                            query_dispatch(
                                {"data_source": query["data_source"], "query": query},
                                notebook["id"],
                                notebook=True,
                            )
                else:
                    if query["data_source"] == "metrics":
                        query_dispatch(
                            {
                                "data_source": query["data_source"],
                                "query": query["query"],
                            },
                            notebook["id"],
                            notebook=True,
                        )
                    elif query["data_source"] in [
                        "logs",
                        "spans",
                        "rum",
                        "security_signals",
                        "profiles",
                        "network",
                        "network_device_flows",
                        "events",
                        "process",
                        "incident_analytics",
                        "apm_sec_spans",
                        "app_sec_spans",
                        "database_queries",
                        "synthetics_batches",
                    ]:
                        query_dispatch(
                            {"data_source": query["data_source"], "query": query},
                            notebook["id"],
                            notebook=True,
                        )


#### Tag finding


def search_metric_tags(query, item_id, category):
    tags = re.findall(METRIC_TAG_REGEX, query)
    resource_tags(item_id, tags, category)


def search_query_tags(query, item_id, category):
    tags = re.findall(QUERY_TAG_REGEX, query)
    resource_tags(item_id, tags, category)


def search_list(query, item_id, category):
    for item in query:
        tags = re.findall(QUERY_TAG_REGEX, item)
        resource_tags(item_id, tags, category)


def resource_tags(dash_id, tags, category):
    if not tags:
        return
    if dash_id in category:
        category[dash_id].extend(tags)
    else:
        category[dash_id] = tags


#### Widget processing


def handle_events(widget, dash_id):
    search_query_tags(widget["definition"]["query"], dash_id, DASHBOARDS)


def handle_hostmap(widget, dash_id):
    search_metric_tags(
        widget["definition"]["requests"]["fill"]["q"], dash_id, DASHBOARDS
    )
    if "scope" in widget["definition"]:
        for item in widget["definition"]["scope"]:
            if "service" in item:
                resource_tags(dash_id, [item], DASHBOARDS)


def handle_list_stream(widget, dash_id):
    for request in widget["definition"]["requests"]:
        search_query_tags(request["query"]["query_string"], dash_id, DASHBOARDS)


def handle_manage_status(widget, dash_id):
    search_query_tags(widget["definition"]["query"], dash_id, DASHBOARDS)


def handle_scatterplot(widget, dash_id):
    if table := widget.get("definition", {}).get("requests", {}).get("table", {}):
        for request_query in table.get("queries", []):
            query_dispatch(request_query, dash_id)


def handle_timeseries(widget, dash_id):
    for request in widget["definition"]["requests"]:
        for request_query in request.get("queries", []):
            query_dispatch(request_query, dash_id)


def handle_check_status(widget, dash_id):
    search_list(widget["definition"]["tags"], dash_id, DASHBOARDS)


def handle_log_stream(widget, dash_id):
    search_query_tags(widget["definition"]["query"], dash_id, DASHBOARDS)


def handle_topo_map(widget, dash_id):
    resource_tags(
        dash_id,
        [f"service:{widget['definition']['requests'][0]['query']['service']}"],
        DASHBOARDS,
    )


def handle_trace_service(widget, dash_id):
    resource_tags(dash_id, [f"service:{widget['definition']['service']}"], DASHBOARDS)


def handle_service_map(widget, dash_id):
    search_list(widget["definition"]["filters"], dash_id, DASHBOARDS)


def parse_query(query, dash_id, notebook=False):
    if notebook:
        search_query_tags(query["query"]["search"]["query"], dash_id, NOTEBOOKS)
    else:
        search_query_tags(query["search"]["query"], dash_id, DASHBOARDS)


def parse_rum(query, dash_id, notebook=False):
    if notebook:
        search_query_tags(query["search"]["query"]["query_string"], dash_id, NOTEBOOKS)
    else:
        search_query_tags(query["search"]["query"]["query_string"], dash_id, DASHBOARDS)


def metrics_query(query, dash_id, notebook=False):
    if notebook:
        search_metric_tags(query["query"], dash_id, NOTEBOOKS)
    else:
        search_metric_tags(query["query"], dash_id, DASHBOARDS)


def process_query(query, dash_id, notebook=False):
    if notebook:
        search_query_tags(query["query"]["query_filter"], dash_id, NOTEBOOKS)
    else:
        search_query_tags(query["query_filter"], dash_id, DASHBOARDS)


def unsupported(widget, dash_id):
    pass


def generic_alert(monitor, monitor_id):
    search_query_tags(monitor["query"], monitor_id, MONITORS)


def synthetics_alert(monitor, monitor_id):
    search_list(monitor["tags"], monitor_id, MONITORS)


def rum_alert(monitor, monitor_id):
    if "variables" in monitor:
        for var in monitor["variables"]:
            search_query_tags(var["search"]["query"], monitor_id, MONITORS)
    else:
        search_query_tags(monitor["query"], monitor_id, MONITORS)


#### Type dispatching


def monitor_type_dispatch(monitor, monitor_id):
    monitor_types = {
        "audit alert": generic_alert,
        "composite": unsupported,
        "error-tracking alert": generic_alert,
        "synthetics alert": synthetics_alert,
        "log alert": generic_alert,
        "slo alert": unsupported,
        "event-v2 alert": generic_alert,
        "trace-analytics alert": generic_alert,
        "metric alert": generic_alert,
        "rum alert": rum_alert,
        "process alert": generic_alert,
        "service check": generic_alert,
        "query alert": generic_alert,
    }
    monitor_types.get(monitor.get("type", {}), unsupported)(monitor, monitor_id)
    print(f"Collected tags for {len(MONITORS)} monitors so far", end="\r")


def widget_dispatch(widget, dash_id):
    widget_types = {
        "change": handle_timeseries,
        "check_status": handle_check_status,
        "distribution": handle_timeseries,
        "event_timeline": handle_events,
        "event_stream": handle_events,
        "funnel": handle_timeseries,
        "geomap": handle_timeseries,
        "heatmap": handle_timeseries,
        "hostmap": handle_hostmap,
        "list_stream": handle_list_stream,
        "log_stream": handle_log_stream,
        "manage_status": handle_manage_status,
        "query_table": handle_timeseries,
        "query_value": handle_timeseries,
        "scatterplot": handle_scatterplot,
        "servicemap": handle_service_map,
        "slo_list": handle_list_stream,
        "sunburst": handle_timeseries,
        "timeseries": handle_timeseries,
        "toplist": handle_timeseries,
        "topology_map": handle_topo_map,
        "trace_service": handle_trace_service,
        "treemap": handle_timeseries,
    }
    widget_types.get(widget.get("definition", {}).get("type"), unsupported)(
        widget, dash_id
    )
    print(f"Collected tags for {len(DASHBOARDS)} dashboards so far", end="\r")


def query_dispatch(request, dash_id, notebook=False):
    queries = {
        "metrics": metrics_query,
        "spans": parse_query,
        "logs": parse_query,
        "rum": parse_query,
        "rum_issue_stream": parse_rum,
        "security_signals": parse_query,
        "profiles": parse_query,
        "network": parse_query,
        "network_device_flows": parse_query,
        "events": parse_query,
        "process": process_query,
        "incident_analytics": parse_query,
        "app_sec_spans": parse_query,
        "database_queries": parse_query,
        "synthetics_batches": parse_query,
        "synthetics_test_runs": parse_query,
        "ci_pipelines": parse_query,
        "ci_tests": parse_query,
        "cloud_cost": metrics_query,
        "audit": parse_query,
    }
    if notebook:
        queries.get(request.get("data_source"))(request, dash_id, notebook=True)
    else:
        queries.get(request.get("data_source"))(request, dash_id)


#### Deduplication


def dedupe_dashboards():
    for dashboard, tags_list in DASHBOARDS.items():
        temp_tags = []
        for tag_set in tags_list:
            tags = tag_set.split(",")
            for tag in tags:
                if "service" in tag:
                    tag_content = tag.split(":")
                    if len(tag_content) == 2:
                        if "$" not in tag_content[1]:
                            temp_tags.append(tag_content[1])
        if temp_tags:
            DEDUPED_DASHBOARDS[dashboard] = list(set(temp_tags))


def dedupe_monitors():
    for monitor, tags_list in MONITORS.items():
        temp_tags = []
        for tag in tags_list:
            temp_tags.append(tag.split(":")[1])
        if temp_tags:
            DEDUPED_MONITORS[monitor] = list(set(temp_tags))


def dedupe_notebooks():
    for notebook, tags_list in NOTEBOOKS.items():
        temp_tags = []
        for tag_set in tags_list:
            tags = tag_set.split(",")
            for tag in tags:
                if "service" in tag:
                    tag_content = tag.split(":")
                    if len(tag_content) == 2:
                        if "$" not in tag_content[1]:
                            temp_tags.append(tag_content[1])
        if temp_tags:
            DEDUPED_NOTEBOOKS[notebook] = list(set(temp_tags))


def dedupe_slos():
    for slo, tags_list in SLOS.items():
        temp_tags = []
        for tag in tags_list:
            temp_tags.append(tag.split(":")[1])
        DEDUPED_SLOS[slo] = list(set(temp_tags))


def generate_output():
    print("********** DASHBOARDS **********")
    for dash, tags in DEDUPED_DASHBOARDS.items():
        print(f"Dashboard: https://app.datadoghq.com/dashboard/{dash}")
        print(f'\tContains services: {", ".join(tags)}')
    print("********************************\n")

    print("*********** MONITORS **********")
    for monitor, tags in DEDUPED_MONITORS.items():
        print(f"Monitor: https://app.datadoghq.com/monitors/{monitor}")
        print(f'\tContains services: {", ".join(tags)}')
    print("*******************************\n")

    print("********** SLOS **********")
    print(
        "Note: SLOs based off of monitors will be reflected in the MONITORS section; only metric SLOs are represented here."
    )
    for slo, tags in DEDUPED_SLOS.items():
        print(f"SLO: https://app.datadoghq.com/slo/manage?slo_id={slo}")
        print(f'\tContains services: {", ".join(tags)}')
    print("*******************************\n")

    print("********** NOTEBOOKS **********")
    for notebook, tags in DEDUPED_NOTEBOOKS.items():
        print(f"Notebook: https://app.datadoghq.com/notebook/{notebook}")
        print(f'\tContains services: {", ".join(tags)}')


if __name__ == "__main__":
    get_all_notebooks()
    dedupe_notebooks()
    get_all_monitors()
    dedupe_monitors()
    get_all_slos()
    dedupe_slos()
    dashboards = get_all_dashboard_ids()
    for dashboard in dashboards:
        get_dashboard_requests(dashboard)
    dedupe_dashboards()
    generate_output()
