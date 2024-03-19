import pyarrow as pa
import pandas as pd
import pyarrow.flight as fl

# Define the host and port of the Arrow Flight server
host = 'localhost'
port = 3000

# Create a Flight client connecting to the server
client = fl.FlightClient(f"grpc://{host}:{port}")

try:
    # Attempt to list available flights
    flights = client.list_flights()
    print("Connection to the Arrow Flight server successful.")
except Exception as e:
    print("Failed to connect to the Arrow Flight server:", e)

# Define your data
data = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35}
]

# Create a DataFrame from the data
df = pd.DataFrame(data)


table = pa.Table.from_pandas(df)

# Define a FlightDescriptor
descriptor = fl.FlightDescriptor.for_path("/example")

# Start a new flight with the FlightDescriptor and schema
writer, metadata_reader = client.do_put(descriptor, table.schema)

# Write the RecordBatch to the Flight stream
writer.write_table(table)

# Close the Flight client connection
client.close()
