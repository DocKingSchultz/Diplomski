import pyarrow as pa
import pandas as pd
import configparser

import csv
import json
from typing import List, Dict
import pandas as pd
import pyarrow as pa

class ParseAndCreateTable:
    def __init__(self, chunk_size: int, data_file_path: str):
        self.chunk_size = chunk_size
        self.data_file_path = data_file_path
        self.data_table_chunked = []

    def read_csv_file(self, delimiter: str = ',') -> List[Dict[str, str]]:
        try:
            with open(self.data_file_path, 'r') as file:
                reader = csv.DictReader(file, delimiter=delimiter)
                data_rows = list(reader)
            return data_rows
        except FileNotFoundError:
            raise FileNotFoundError(f"File '{self.data_file_path}' not found.")
        except Exception as e:
            raise Exception(f"Error reading file '{self.data_file_path}': {e}")

    def chunk_data(self, data: List[Dict[str, str]]) -> List[List[Dict[str, str]]]:
        return [data[i:i + self.chunk_size] for i in range(0, len(data), self.chunk_size)]

    def initialize_arrow_table(self):
        data_rows = self.read_csv_file()
        chunked_data = self.chunk_data(data_rows)
        for chunk in chunked_data:
            df = pd.DataFrame(chunk)
            table = pa.Table.from_pandas(df)
            self.data_table_chunked.append(table)

    def initialize_json_table(self):
        data_rows = self.read_csv_file()
        chunked_data = self.chunk_data(data_rows)
        for chunk in chunked_data:
                self.data_table_chunked.append(chunk)
