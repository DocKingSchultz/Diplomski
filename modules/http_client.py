import requests
import time
import configparser
from data_parser import ParseAndCreateTable  # Assuming you have this module

class HttpDataSender:
    def __init__(self, properties_file):
        self.properties_file = properties_file
        self.host, self.port, self.data_file_path, self.chunk_size = self.read_properties()
        self.base_url = f"http://{self.host}:{self.port}"
        self.table_initializer = ParseAndCreateTable(self.chunk_size, self.data_file_path)

    def read_properties(self):
        config = configparser.ConfigParser()
        config.read(self.properties_file)
        host = config['SETTINGS']['host']
        port = int(config['SETTINGS']['http_port'])
        data_file_path = config['SETTINGS']['data_file_path']
        chunk_size = int(config['SETTINGS']['chunk_size'])
        return host, port, data_file_path, chunk_size

    def start_data_transaction(self):
        try:
            url = f"{self.base_url}/startDataTransaction"
            headers = {'Content-Type': 'application/json'}
            payload = {
                    "op": "startDataTransaction",
                    "numberOfTableRowsInBatch": len(self.table_initializer.data_table_chunked[0]),
                    "numberOfBatches": len(self.table_initializer.data_table_chunked),
            }
            response = requests.post(url, json=payload, headers=headers)
            response_data = response.json()
            if response.status_code == 200 and response_data.get('status') == 'Transaction initialized':
                print(f"Transaction initialzed properly.")
                self.send_batches()
            else:
                print("Error: Starting with data transaction failed")
                print("Response:", response_data)
        except Exception as e:
            print("Error sending HTTP request:", e)
    def send_batches(self):
        try:
            url = f"{self.base_url}/sendBatches"
            headers = {'Content-Type': 'application/json'}

            # Record start time
            start_time = time.time()

            for i, batch in enumerate(self.table_initializer.data_table_chunked):
                payload = {
                    "op": "sendingBatches",
                    "numberOfTableRowsInBatch": len(batch),
                    "numberOfBatches": len(self.table_initializer.data_table_chunked),
                    "batch": batch
                }
                response = requests.post(url, json=payload, headers=headers)
                response_data = response.json()
                if response.status_code == 200 and response_data.get('status') == 'Success':
                    #print(f"Batch {i+1}/{len(self.table_initializer.data_table_chunked)} sent successfully.")
                    pass
                else:
                    print("Error: Server response indicates failure for batch", i+1)
                    print("Response:", response_data)
                    return

            # Record end time
            end_time = time.time()

            # Calculate elapsed time
            elapsed_time = (end_time - start_time) * 1000  # Convert to milliseconds
            print('Elapsed time:', f'{elapsed_time:.2f}', 'milliseconds')

        except Exception as e:
            print("Error sending HTTP request:", e)

# Example usage:
properties_file = '../config.properties'
sender = HttpDataSender(properties_file)
sender.table_initializer.initialize_json_table()
sender.start_data_transaction()
