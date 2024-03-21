import pyarrow as pa
import pandas as pd
import pyarrow.flight as fl
import time

# Define the host and port of the Arrow Flight server
host = 'localhost'
port = 3000

# Create a Flight client connecting to the server
client = fl.FlightClient(f"grpc://{host}:{port}")

# Read data from the file 'data-table.txt'
data_file_path = 'resources/data_csv_1000.csv'
with open(data_file_path, 'r') as file:
    lines = file.readlines()

# Extract column names from the first line
column_names = lines[0].strip().split(',')

# Parse data rows
data_rows = []
for line in lines[1:]:
    values = line.strip().split(',')
    data_rows.append(dict(zip(column_names, values)))

# Create a DataFrame from the data
df = pd.DataFrame(data_rows)

# Convert DataFrame to Arrow Table
table = pa.Table.from_pandas(df)

# Define a FlightDescriptor
descriptor = fl.FlightDescriptor.for_path("/example")

try:
    # Attempt to exchange data with the server
    start_time = time.time()  # Start calculating time here
    writer, reader = client.do_exchange(descriptor)

    try:
        # Initialize the writer with the schema
        writer.begin(table.schema)

        # Write the RecordBatch to the Flight stream
        writer.write_table(table)

        # Close the writer
        writer.done_writing()
        
        # Read the response from the server
        response_table = reader.read_all()

        # Stop calculating time
        end_time = time.time()

        # Calculate elapsed time
        elapsed_time = (end_time - start_time) * 1000  # Convert to milliseconds


        print("Data exchange successful.")
        #print(response_table.to_pandas())
        print('Elapsed time:', f'{elapsed_time:.2f}', 'milliseconds')

    except Exception as e:
        print("Error during data exchange:", e)

except Exception as e:
    print("Error connecting to server or exchanging data:", e)

finally:
    # Close the Flight client connection
    client.close()