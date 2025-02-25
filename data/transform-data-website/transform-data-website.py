from google.cloud import bigquery
import json

# Initialize BigQuery client
client = bigquery.Client()

# Query nodes
nodes_query = """
    SELECT node_id, lat, lon
    FROM `trail-master-447216.staging.nodes`
"""
nodes_df = client.query(nodes_query).to_dataframe()

# Convert nodes to a dictionary
nodes_dict = {row["node_id"]: (row["lon"], row["lat"]) for _, row in nodes_df.iterrows()}

# Query ways
ways_query = """
    SELECT way_id, nodes
    FROM `trail-master-447216.staging.ways`
"""
ways_df = client.query(ways_query).to_dataframe()

# Convert ways to GeoJSON format
geojson_features = []

for _, row in ways_df.iterrows():
    way_id = row["way_id"]
    coordinates = [nodes_dict[node] for node in row["nodes"] if node in nodes_dict]

    if coordinates:
        geojson_features.append({
            "type": "Feature",
            "properties": {"way_id": way_id},
            "geometry": {"type": "LineString", "coordinates": coordinates}
        })

geojson_data = {
    "type": "FeatureCollection",
    "features": geojson_features
}

# Save to a GeoJSON file with indentation
with open("trails.geojson", "w") as f:
    json.dump(geojson_data, f, indent=4)  # Added indent=4 for pretty formatting
