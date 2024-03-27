import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import threading

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
        print("Data transaction finished.")
        print("---------------------------------------------------------------------")

class HTTPRequestHandler(BaseHTTPRequestHandler):
    data_table = []

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        raw_bytes = self.rfile.read(content_length)
        self.data_table.append(raw_bytes)
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response_data = {'status': 'success', 'message': 'Data received successfully'}
        self.wfile.write(json.dumps(response_data).encode())

    def do_GET(self):
        if self.path == '/get_table':
            concatenated_data = b"".join(self.data_table)
            self.send_response(200)
            self.send_header('Content-type', 'text/csv')
            self.end_headers()
            self.wfile.write(concatenated_data)
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"404 - Not Found")

def start_flight_server():
    server = MyFlightServer(('0.0.0.0', 3000))
    print("Starting Flight server on port 3000")
    server.serve()

def start_http_server():
    server_address = ('', 8080)
    httpd = HTTPServer(server_address, HTTPRequestHandler)
    print("Starting HTTP server on port 8080")
    httpd.serve_forever()

if __name__ == "__main__":
    flight_server_thread = threading.Thread(target=start_flight_server)
    flight_server_thread.start()
    start_http_server()
