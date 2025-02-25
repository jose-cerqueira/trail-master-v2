from google.cloud import bigquery
import requests
import pandas as pd
import time

# Configuration
PROJECT_ID = "trail-master-447216"
DATASET_ID = "staging"
TABLE_ID_WAYS = "ways"
TABLE_ID_NODES = "nodes"

def call_api():
    query = """
    [out:json][timeout:25];
    (
    way(around:2000,52.521909,13.413265)
    ["highway"~"path|track|pedestrian"];
    );
    foreach(
    way._(if:length() > 300);
    out body;
    >;
    out skel qt;
    );
    """
    url = "https://overpass-api.de/api/interpreter"
    response = requests.get(url, data={"data": query})

    return response.json().get("elements", []) if response.status_code == 200 else []

def split_way_node():
    data = call_api()
    ways, nodes = [], {}

    for element in data:
        if element["type"] == "way":
            ways.append({
                "way_id": element["id"],
                "nodes": element["nodes"],
                "highway": element["tags"].get("highway"),
                "surface": element["tags"].get("surface"),
                "bicycle": element["tags"].get("bicycle"),
                "area": element["tags"].get("area"),
                "lit": element["tags"].get("lit"),
                "smoothness": element["tags"].get("smoothness"),
                "name": element["tags"].get("name"),
                "wikidata": element["tags"].get("wikidata"),
                "wikimedia_commons": element["tags"].get("wikimedia_commons"),
                "wikipedia": element["tags"].get("wikipedia"),
            })
        elif element["type"] == "node":
            nodes[element["id"]] = {
                "node_id": element["id"],
                "lat": element["lat"],
                "lon": element["lon"]
            }

    return pd.DataFrame(ways), pd.DataFrame(nodes.values())

def insert_table_bigquery(table_id, df, schema):
    client = bigquery.Client()
    table_ref = client.dataset(DATASET_ID).table(table_id)

    if "nodes" in df.columns:
        df["nodes"] = df["nodes"].apply(lambda x: list(map(int, x)) if isinstance(x, list) else [])

    client.delete_table(table_ref, not_found_ok=True)
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)

    time.sleep(5)
    job = client.load_table_from_dataframe(df, table_ref, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
    job.result()

def ingest_trails(request):
    ways_df, nodes_df = split_way_node()

    ways_schema = [
        bigquery.SchemaField("way_id", "INTEGER"),
        bigquery.SchemaField("nodes", "INTEGER", mode="REPEATED"),
        bigquery.SchemaField("highway", "STRING"),
        bigquery.SchemaField("surface", "STRING"),
        bigquery.SchemaField("bicycle", "STRING"),
        bigquery.SchemaField("area", "STRING"),
        bigquery.SchemaField("lit", "STRING"),
        bigquery.SchemaField("smoothness", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("wikidata", "STRING"),
        bigquery.SchemaField("wikimedia_commons", "STRING"),
        bigquery.SchemaField("wikipedia", "STRING"),
    ]

    nodes_schema = [
        bigquery.SchemaField("node_id", "INTEGER"),
        bigquery.SchemaField("lat", "FLOAT"),
        bigquery.SchemaField("lon", "FLOAT"),
    ]

    insert_table_bigquery(TABLE_ID_WAYS, ways_df, ways_schema)
    insert_table_bigquery(TABLE_ID_NODES, nodes_df, nodes_schema)

    return "BigQuery insertion successful", 200
