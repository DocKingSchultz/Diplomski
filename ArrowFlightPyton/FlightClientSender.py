import pyarrow as pa
import pandas as pd
import pyarrow.flight as fl

# Define the host and port of the Arrow Flight server
host = 'localhost'
port = 3000

# Create a Flight client connecting to the server
client = fl.FlightClient(f"grpc://{host}:{port}")

# Define your data
data = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35}
]

# Create a schema for the data
schema = pa.schema([
    ("name", pa.string()),
    ("age", pa.int32())
])

# Convert data to Arrow RecordBatch
record_batch = pa.RecordBatch.from_pandas(pd.DataFrame(data), schema=schema)

# Define a FlightDescriptor
descriptor = fl.FlightDescriptor.for_path("/example")

# Define a list of FlightEndpoints (if available)
endpoints = []

# Create a FlightInfo object with the schema and other metadata
info = fl.FlightInfo(schema, descriptor, endpoints, -1, -1)

# Start a new flight with the FlightInfo
writer, metadata_reader = client.do_put(descriptor, schema)

# Write the RecordBatch to the Flight stream
writer.write(record_batch)
writer.close()

# Close the Flight client connection
client.close()
