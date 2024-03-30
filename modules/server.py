import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import threading
import logging
import os
import configparser
import csv
from datetime import datetime

# Disable logging
logging.basicConfig(level=logging.CRITICAL)

# Propetries file with 
properties_file = "../config.properties"

class MyFlightServer(flight.FlightServerBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = {"numberOfTableRowsInBatch": None, "numberOfBatches": None, "tables": []}

    def initialize_data(self):
        self.data = {"numberOfTableRowsInBatch": None, "numberOfBatches": None, "tables": []}

    def do_put(self, context, descriptor, reader, writer):
        try:
            self.initialize_data()
            message_table = reader.read_all()
            message_df = message_table.to_pandas()
            message_dict = message_df.to_dict(orient='records')[0]

            if message_dict.get('op') == 'sendingBatches':
                self.handle_sending_batches(message_dict)
            else:
                print("Unexpected message received.")
        except Exception as e:
            print("Error occurred during data reception:", e)

    def handle_sending_batches(self, message_dict):
        self.data['numberOfTableRowsInBatch'] = message_dict['numberOfTableRowsInBatch']
        self.data['numberOfBatches'] = message_dict['numberOfBatches']
        print("Received message from arrow client:", message_dict)

    def do_exchange(self, context, descriptor, reader, writer):
        message = {
            "status": "Failed",
            "description": "",
            "numberOfBatchesRecevied": 0,
            "numberOfRowsInBatches": 0
        }
        try:
            schema = reader.schema
            total_batches = self.data.get('numberOfBatches')

            if total_batches is None:
                raise Exception("numberOfBatches is not set properly on server side")

            record_batch_reader = reader.to_reader()
            num_batches_read = 0

            while num_batches_read < total_batches:
                batch = record_batch_reader.read_next_batch()
                self.data['tables'].append(batch)
                num_batches_read += 1
                print("Data chunk from arrow client received number of batch : ", num_batches_read)

            if len(self.data['tables']) == total_batches:
                self.validate_batches(message)
            else:
                print("Error: Number of received batches does not match numberOfBatches.")
                message['description'] = "Received data does not match the expected ones"

        except Exception as e:
            print("Error occurred during data exchange:", e)
            message['description'] = str(e)

        finally:
            self.send_message_data(message, writer)

    def validate_batches(self, message):
        num_rows_per_batch = self.data.get('numberOfTableRowsInBatch')
        for batch in self.data['tables']:
            if len(batch) != num_rows_per_batch:
                raise Exception("Incorrect number of rows in one or more batches")

        message['numberOfBatchesRecevied'] = len(self.data['tables'])
        message['numberOfRowsInBatches'] = len(self.data['tables'][0])
        message['status'] = "Success"

    def send_message_data(self, message, writer):
        message_df = pd.DataFrame([message])
        message_table = pa.Table.from_pandas(message_df)
        schema = message_table.schema
        writer.begin(schema)
        writer.write_table(message_table)
        writer.close()
        print("Data transaction with arrow client finished.")
        print("---------------------------------------------------------------------")

class HTTPRequestHandler(BaseHTTPRequestHandler):
    data = {"numberOfTableRowsInBatch": None, "numberOfBatches": None, "tables": []}

    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            json_data = json.loads(post_data.decode('utf-8'))
            
            operation_handlers = {
                'sendingBatches': self.handle_sending_batches,
                'startDataTransaction': self.handle_start_data_transaction,
                'transactionFinished': self.handle_transaction_finished
            }
            
            operation = json_data.get('op')
            handler = operation_handlers.get(operation)
            if handler:
                handler(json_data)
            else:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Invalid operation'}).encode())
                
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode())

    def handle_start_data_transaction(self, json_data):
        self.reset_data()
        self.data['numberOfTableRowsInBatch'] = json_data['numberOfTableRowsInBatch']
        self.data['numberOfBatches'] = json_data['numberOfBatches']
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'Transaction initialized'}).encode())
        print("Transaction with http client initialized.")
        pass

    def handle_transaction_finished(self, json_data):
        # Handle transactionFinished operation
        pass
    
    def handle_sending_batches(self, json_data):
        try:
            if 'batch' in json_data:
                self.data['tables'].append(json_data['batch'])
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'status': 'Success'}).encode())
                print("Batch received successfully.")
                print(len(self.data['tables']), self.data['numberOfBatches'])
                #print(json_data['batch'])
                if len(self.data['tables']) == self.data['numberOfBatches']:
                    # All batches received, reset data
                    config = configparser.ConfigParser()
                    config.read(properties_file)
                    logs_active = bool(config['VARIABLES']['logs_active'])
                    logs_dir = config['VARIABLES']['logs_dir']
                    print(logs_active, logs_dir)
                    if(logs_active) :
                        self.save_table_to_log(logs_dir)
                    self.reset_data()
                    print("All batches received. Data reset.")
            else:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Invalid payload'}).encode())
        except Exception as e:
            print(e)
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode())


    def reset_data(self):
        self.data['numberOfTableRowsInBatch'] = None
        self.data['numberOfBatches'] = None
        self.data['tables'] = []

    def save_table_to_log(self, log_dir):
        print("Logging, needs rework")
        # timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # log_file = os.path.join(log_dir, f"table_{timestamp}.csv")
        
        # # Create the directory if it doesn't exist
        # os.makedirs(log_dir, exist_ok=True)
        
        # with open(log_file, "w", newline='') as f:
        #     writer = csv.writer(f)
        #     df = pd.DataFrame(self.data['tables'])
        #     # Write the DataFrame to CSV
        #     df.to_csv(log_file, index=False)
        # print("Log file created:", log_file)


    

def start_http_server(host, port):
    server_address = (host, port)
    httpd = HTTPServer(server_address, HTTPRequestHandler)
    print("Starting HTTP server on port : ", port)
    httpd.serve_forever()

def start_flight_server(host, port):
    server = MyFlightServer((host, port))
    print("Starting Flight server on port : ", port)
    server.serve()

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(properties_file)
    host = config['SETTINGS']['host']
    arrow_port = int(config['SETTINGS']['arrow_port'])
    http_port = int(config['SETTINGS']['http_port'])

    flight_server_thread = threading.Thread(target=start_flight_server, args=(host, arrow_port))
    flight_server_thread.start()
    start_http_server(host, http_port)
