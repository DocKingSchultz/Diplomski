import subprocess
import sys
import os
import csv
import time
import socket
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent
MODULES_DIR = ROOT / 'modules'
RESOURCES_DIR = ROOT / 'resources'
RESULTS_DIR = ROOT / 'results'
PROPERTIES_FILE = str(ROOT / 'config.properties')

sys.path.insert(0, str(MODULES_DIR))
from arrow_client import FlightDataSender
from http_client import HttpDataSender
from data_parser import ParseAndCreateTable

# --- Experiment configuration ---

N_RUNS = 5      # measured runs per configuration
N_WARMUP = 1    # warm-up runs discarded before measuring

# Experiment 1: how protocols scale with data volume (fixed chunk size)
EXP1_CHUNK_SIZE = 1000
EXP1_DATASETS = [
    ('data_csv_10.csv',   10),
    ('data_csv_100.csv',  100),
    ('data_csv_1k.csv',   1_000),
    ('data_csv_10k.csv',  10_000),
    ('data_csv_100k.csv', 100_000),
    ('data_csv_1m.csv',   1_000_000),
]

# Experiment 2: how chunk size affects each protocol (fixed dataset)
EXP2_DATASET = ('data_csv_100k.csv', 100_000)
EXP2_CHUNK_SIZES = [100, 500, 1000, 5000, 10000, 50000]

# --------------------------------

def wait_for_port(host, port, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (OSError, ConnectionRefusedError):
            time.sleep(0.5)
    return False

def start_server():
    proc = subprocess.Popen(
        [sys.executable, str(MODULES_DIR / 'server.py')],
        cwd=str(MODULES_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print("Waiting for server to start...", end='', flush=True)
    ok_arrow = wait_for_port('localhost', 3000)
    ok_http  = wait_for_port('localhost', 8080)
    if not (ok_arrow and ok_http):
        proc.kill()
        raise RuntimeError("Server did not start in time")
    print(" ready.")
    return proc

def get_file_size_mb(filename):
    path = RESOURCES_DIR / filename
    return path.stat().st_size / (1024 * 1024)

def load_arrow_data(filename, chunk_size):
    sender = FlightDataSender(
        PROPERTIES_FILE,
        chunk_size=chunk_size,
        data_file_path=str(RESOURCES_DIR / filename),
    )
    sender.table_initializer.initialize_arrow_table()
    return sender

def load_http_data(filename, chunk_size):
    sender = HttpDataSender(
        PROPERTIES_FILE,
        chunk_size=chunk_size,
        data_file_path=str(RESOURCES_DIR / filename),
    )
    sender.table_initializer.initialize_json_table()
    return sender

def run_arrow_once(sender):
    # Recreate the Flight client (previous run closed it)
    import pyarrow.flight as fl
    sender.client = fl.FlightClient(f"grpc://{sender.host}:{sender.port}")
    sender.startCommunication()
    return sender.send_batches()

def run_http_once(sender):
    return sender.start_data_transaction()

def benchmark(protocol, filename, num_rows, chunk_size):
    file_size_mb = get_file_size_mb(filename)
    num_batches = -(-num_rows // chunk_size)  # ceiling division

    print(f"  [{protocol.upper()}] {filename} | rows={num_rows:>8} | chunk={chunk_size:>6} | batches={num_batches:>5}", end='', flush=True)

    if protocol == 'arrow':
        sender = load_arrow_data(filename, chunk_size)
        run_once = lambda: run_arrow_once(sender)
    else:
        sender = load_http_data(filename, chunk_size)
        run_once = lambda: run_http_once(sender)

    # Warm-up
    for _ in range(N_WARMUP):
        t = run_once()
        if t is None:
            print(" FAILED (warm-up)")
            return []

    # Measured runs
    records = []
    for run_idx in range(N_RUNS):
        elapsed_ms = run_once()
        if elapsed_ms is None:
            print(f" FAILED (run {run_idx + 1})")
            return records
        throughput = (file_size_mb / (elapsed_ms / 1000)) if elapsed_ms > 0 else 0
        records.append({
            'protocol':       protocol,
            'dataset':        filename,
            'num_rows':       num_rows,
            'chunk_size':     chunk_size,
            'num_batches':    num_batches,
            'file_size_mb':   round(file_size_mb, 4),
            'run':            run_idx + 1,
            'elapsed_ms':     round(elapsed_ms, 2),
            'throughput_mbs': round(throughput, 4),
        })
        print('.', end='', flush=True)

    times = [r['elapsed_ms'] for r in records]
    avg = sum(times) / len(times)
    print(f"  avg={avg:.0f}ms")
    return records

def write_results(all_records, out_file):
    RESULTS_DIR.mkdir(exist_ok=True)
    fieldnames = ['protocol', 'dataset', 'num_rows', 'chunk_size', 'num_batches',
                  'file_size_mb', 'run', 'elapsed_ms', 'throughput_mbs']
    with open(out_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_records)
    print(f"\nResults saved to: {out_file}")

def restart_server(proc):
    proc.kill()
    proc.wait()
    time.sleep(1)
    return start_server()

def main():
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_file = RESULTS_DIR / f'benchmark_{ts}.csv'

    server_proc = start_server()
    all_records = []

    try:
        # --- Experiment 1 ---
        print(f"\n{'='*60}")
        print(f"EXPERIMENT 1 — Scalability (chunk_size={EXP1_CHUNK_SIZE})")
        print(f"{'='*60}")
        for filename, num_rows in EXP1_DATASETS:
            for protocol in ['arrow', 'http']:
                records = benchmark(protocol, filename, num_rows, EXP1_CHUNK_SIZE)
                all_records.extend(records)
                time.sleep(0.5)

        # Fresh server for Experiment 2 — eliminates accumulated state from Exp 1
        print("\nRestarting server before Experiment 2...")
        server_proc = restart_server(server_proc)

        # --- Experiment 2 ---
        print(f"\n{'='*60}")
        print(f"EXPERIMENT 2 — Chunk size impact (dataset={EXP2_DATASET[0]})")
        print(f"{'='*60}")
        filename, num_rows = EXP2_DATASET
        for chunk_size in EXP2_CHUNK_SIZES:
            for protocol in ['arrow', 'http']:
                records = benchmark(protocol, filename, num_rows, chunk_size)
                all_records.extend(records)
                time.sleep(0.5)

    finally:
        server_proc.kill()
        print("\nServer stopped.")

    write_results(all_records, out_file)

if __name__ == '__main__':
    main()
