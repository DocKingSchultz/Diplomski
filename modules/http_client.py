import requests
import time
import configparser
from data_parser import ParseAndCreateTable  # Assuming you have this module

class HttpDataSender:
    def __init__(self, properties_file, chunk_size=None, data_file_path=None):
        self.properties_file = properties_file
        self.host, self.port, cfg_data_file, cfg_chunk_size = self.read_properties()
        self.chunk_size = chunk_size if chunk_size is not None else cfg_chunk_size
        self.data_file_path = data_file_path if data_file_path is not None else cfg_data_file
        self.base_url = f"http://{self.host}:{self.port}"
        self.table_initializer = ParseAndCreateTable(self.chunk_size, self.data_file_path)
        self.session = requests.Session()

    def read_properties(self):
        config = configparser.ConfigParser()
        config.read(self.properties_file)
        host = config['SETTINGS']['host']
        port = int(config['SETTINGS']['http_port'])
        data_file_path = config['VARIABLES']['data_file_path']
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
            response = self.session.post(url, json=payload, headers=headers)
            response_data = response.json()
            if response.status_code == 200 and response_data.get('status') == 'Transaction initialized':
                return self.send_batches()
            else:
                print("Error: Starting with data transaction failed")
                print("Response:", response_data)
                return None
        except Exception as e:
            print("Error sending HTTP request:", e)
            return None

    def send_batches(self):
        try:
            url_batches = f"{self.base_url}/sendBatches"
            url_finished = f"{self.base_url}/transactionFinished"
            headers = {'Content-Type': 'application/json'}

            start_time = time.time()

            for i, batch in enumerate(self.table_initializer.data_table_chunked):
                payload = {
                    "op": "sendingBatches",
                    "numberOfTableRowsInBatch": len(batch),
                    "numberOfBatches": len(self.table_initializer.data_table_chunked),
                    "batch": batch
                }
                response = self.session.post(url_batches, json=payload, headers=headers)
                response_data = response.json()
                if not (response.status_code == 200 and response_data.get('status') == 'Success'):
                    print("Error: Server response indicates failure for batch", i + 1)
                    print("Response:", response_data)
                    return None

            # Wait for server to confirm all batches received and processed
            response = self.session.post(url_finished, json={"op": "transactionFinished"}, headers=headers)
            response_data = response.json()

            end_time = time.time()
            elapsed_time = (end_time - start_time) * 1000

            if response.status_code == 200 and response_data.get('status') == 'Success':
                return elapsed_time
            else:
                print("Error: Final transaction confirmation failed")
                return None

        except Exception as e:
            print("Error sending HTTP request:", e)
            return None

if __name__ == '__main__':
    properties_file = '../config.properties'
    sender = HttpDataSender(properties_file)
    sender.table_initializer.initialize_json_table()
    sender.start_data_transaction()
