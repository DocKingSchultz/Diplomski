import pandas as pd
import pyarrow.flight as fl
import pyarrow as pa
import time
from data_parser import ParseAndCreateTable
import configparser

class FlightDataSender:
    def __init__(self, properties_file):
        self.properties_file = properties_file
        self.host, self.port, self.data_file_path, self.chunk_size = self.read_properties()
        self.client = fl.FlightClient(f"grpc://{self.host}:{self.port}")
        self.table_initializer = ParseAndCreateTable(self.chunk_size, self.data_file_path)

    def read_properties(self):
        config = configparser.ConfigParser()
        config.read(self.properties_file)
        host = config['SETTINGS']['host']
        port = int(config['SETTINGS']['arrow_port'])
        data_file_path = config['SETTINGS']['data_file_path']
        chunk_size = int(config['SETTINGS']['chunk_size'])
        return host, port, data_file_path, chunk_size

    def startCommunication(self):
        try:
            # Create message
            message = {
                "op": ["sendingBatches"],
                "numberOfTableRowsInBatch": [len(self.table_initializer.data_table_chunked[0])],  # Number of elements in the first chunk
                "numberOfBatches": [len(self.table_initializer.data_table_chunked)]  # Number of entries in data_table_chunked
            }
            print("numberOfTableRowsInBatch : ", [len(self.table_initializer.data_table_chunked[0])])
            print("numberOfBatches : ", [len(self.table_initializer.data_table_chunked)])
            # Convert message to Arrow Table
            message_df = pd.DataFrame(message)
            message_table = pa.Table.from_pandas(message_df)

            # Get schema from the message table
            schema = message_table.schema

            # Define a FlightDescriptor for the message
            descriptor = fl.FlightDescriptor.for_path("/startCommunication")

            # Attempt to exchange data with the server
            writer, _ = self.client.do_put(descriptor, schema)

            # Write the message to the Flight stream
            writer.write_table(message_table)

            # Close the writer
            writer.close()

            print("Message sent to server successfully.")

        except Exception as e:
            print("Error sending message to server:", e)

    def send_batches(self):
        # Define a FlightDescriptor for sending batches
        descriptor = fl.FlightDescriptor.for_path("/sendBatches")

        try:
            start_time = time.time()
            # Attempt to exchange data with the server
            writer, reader = self.client.do_exchange(descriptor)

            try:
                # Initialize the writer with the schema
                writer.begin(self.table_initializer.data_table_chunked[0].schema)  # Use the schema of the first chunk
                
                # Write each chunk of data to the Flight stream
                for chunk in self.table_initializer.data_table_chunked:
                    writer.write_table(chunk)

                # Close the writer
                writer.done_writing()

                # Read the response from the server
                response = reader.read_all()

                # Convert the message table to a Pandas DataFrame
                message_df = response.to_pandas()
                
                # Convert the DataFrame to a dictionary
                message_dict = message_df.to_dict(orient='records')[0]
                if message_dict.get('status') == 'Success':
                    end_time = time.time()
                    elapsed_time = (end_time - start_time) * 1000  # Convert to milliseconds
                    print("Server has finished processing the batches.")
                    print('Elapsed time:', f'{elapsed_time:.2f}', 'milliseconds')
                print(message_dict)
            except Exception as e:
                print("Error during data exchange:", e)

        except Exception as e:
            print("Error connecting to server or exchanging data:", e)

        finally:
            # Close the Flight client connection
            self.client.close()

# Example usage:
properties_file = '../config.properties'
sender = FlightDataSender(properties_file)
sender.table_initializer.initialize_arrow_table()
sender.startCommunication()
sender.send_batches()
