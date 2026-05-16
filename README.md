# HTTP/JSON vs Apache Arrow Flight (gRPC) — Benchmark

A comparative performance analysis of two data transfer protocols for sending large tabular datasets over a network. The project measures transfer time and throughput for HTTP with JSON serialization against Apache Arrow Flight over gRPC, across different dataset sizes and batching strategies.

---

## Project Structure

```
├── main.py                  # Benchmark runner (entry point)
├── config.properties        # Server host, ports, chunk size
├── modules/
│   ├── server.py            # Dual server: Arrow Flight on :3000, HTTP on :8080
│   ├── arrow_client.py      # Arrow Flight client
│   ├── http_client.py       # HTTP/JSON client
│   └── data_parser.py       # CSV loading and chunking
├── resources/
│   ├── data_csv_10.csv      # 10 rows
│   ├── data_csv_100.csv     # 100 rows
│   ├── data_csv_1k.csv      # 1 000 rows
│   ├── data_csv_10k.csv     # 10 000 rows
│   ├── data_csv_100k.csv    # 100 000 rows
│   └── data_csv_1m.csv      # 1 000 000 rows (~88 MB)
└── results/                 # Generated benchmark CSVs (git-ignored)
```

---

## Dataset

All test datasets share the same schema with mixed column types, chosen deliberately to highlight differences in serialization overhead between JSON (string-based) and Arrow (binary, typed):

| Column     | Type   |
|------------|--------|
| Integer    | int    |
| String     | string |
| Age        | int    |
| Boolean    | bool   |
| BigInteger | int64  |
| BigFloat   | float64|

JSON serializes every value as a string regardless of type (e.g. `"3.14159"`, `"true"`). Arrow encodes each column in its native binary format, resulting in smaller payloads and faster parsing.

---

## How It Works

### Server

`modules/server.py` starts two servers in parallel threads:

- **Arrow Flight server** on port `3000` (gRPC)
- **HTTP server** on port `8080`

Both servers are stateful — they track expected batch count and accumulate received batches until a transaction is complete.

### Arrow Flight side (`modules/arrow_client.py`)

Apache Arrow Flight is a gRPC-based protocol designed specifically for high-throughput transfer of columnar data. Communication happens in two steps:

1. **Handshake (`do_put`)** — client sends metadata to the server: number of batches and rows per batch. The server stores this to know what to expect.

2. **Data transfer (`do_exchange`)** — client opens a single bidirectional gRPC stream. All data chunks are written to the stream sequentially. Once all chunks are written, the client signals it is done writing (`done_writing()`), then waits for the server's confirmation response (`{"status": "Success"}`). The timer covers this entire phase — from opening the stream to receiving the final confirmation.

Data preparation: CSV is read once, converted to a pandas DataFrame, then to a typed PyArrow Table. The table is split into fixed-size chunks (`chunk_size` rows each). Arrow's columnar binary format (IPC) is used on the wire.

### HTTP/JSON side (`modules/http_client.py`)

Communication follows a three-phase request-response pattern over HTTP/1.1:

1. **Handshake (`POST /startDataTransaction`)** — client sends batch count and rows-per-batch metadata. Server acknowledges with `{"status": "Transaction initialized"}`. Timer has not started yet.

2. **Data transfer (`POST /sendBatches`)** — client sends each chunk as a separate JSON POST request and waits for `{"status": "Success"}` before sending the next one. The timer starts before the first batch is sent. Each request carries the full batch payload as a JSON array of row objects.

3. **Completion (`POST /transactionFinished`)** — after the last batch, client sends a final signal and waits for the server to confirm all batches were received. The timer stops after this confirmation.

Data preparation: CSV is read once into a list of row dictionaries (via `csv.DictReader`). All values remain as strings, matching how JSON naturally represents data. Chunks are plain Python lists.

A `requests.Session` is used to reuse the underlying TCP connection across all batch requests, equivalent to HTTP keep-alive. This ensures the benchmark measures serialization and round-trip overhead, not repeated TCP handshake cost.

### Key difference

Arrow streams all chunks over one persistent gRPC connection and receives a single confirmation at the end — **1 round-trip** for the data phase regardless of batch count.

HTTP requires one request-response cycle per batch — **N round-trips** where N = `total_rows / chunk_size`. This is an inherent property of the HTTP request-response model, not an implementation choice.

---

## Running the Benchmark

### Prerequisites

```powershell
# Install Python 3.11 (if not installed)
winget install Python.Python.3.11

# Refresh PATH in current terminal session
$env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("PATH", "User")

# Create and activate virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install pyarrow==15.0.2 pandas==2.2.1 requests==2.31.0
```

### Run

```powershell
python main.py
```

`main.py` automatically starts the server as a subprocess, runs all experiments, and kills the server when done. Results are saved to `results/benchmark_<timestamp>.csv`.

---

## Experiments

### Experiment 1 — Scalability by data volume

Fixed `chunk_size = 1000`. Dataset size varies from 10 to 1 000 000 rows. Answers the question: *how does each protocol scale as the amount of data grows?*

### Experiment 2 — Impact of chunk size

Fixed dataset of 100 000 rows. Chunk size varies: 100, 500, 1 000, 5 000, 10 000, 50 000. Answers the question: *how does the batching strategy affect each protocol's performance?*

This experiment is particularly revealing — Arrow's performance is relatively stable across chunk sizes because the number of round-trips does not change. HTTP's performance degrades significantly with smaller chunks because each chunk requires a separate round-trip.

---

## Measurement Methodology

### Warm-up run

Each configuration runs one warm-up iteration before measurement begins. The warm-up result is discarded. This ensures:

- The OS network stack has an established connection
- Python's import and JIT overhead is absorbed
- The server's internal state is initialized once
- File system caches for the dataset are warm

Without a warm-up, the first measured run would include one-time setup latency not representative of steady-state performance.

### Measured runs

After the warm-up, `N_RUNS = 10` consecutive runs are performed for each configuration. Each run re-establishes the Flight client connection (Arrow) or reuses the HTTP session (HTTP), but data is **not re-read from disk** — the parsed and chunked dataset stays in memory across all runs.

10 runs were chosen over a smaller number because isolated OS scheduling spikes (context switches, garbage collection pauses) can inflate individual measurements significantly. With 5 runs, a single such spike shifts the average by 20%. With 10 runs, its weight is halved, and the trimmed mean eliminates it entirely.

### Server restart between experiments

The server is restarted between Experiment 1 and Experiment 2. Without a restart, the server accumulates received data in memory across all runs of Experiment 1. For large datasets (1M rows, ~84 MB), this causes Python's garbage collector to interfere with Experiment 2 timing — producing results that are not comparable to a fresh server state. The restart ensures each experiment starts from a clean memory baseline.

### Representative value — trimmed mean

The raw results file contains every individual run. The summary file derives one representative value per configuration using the **trimmed mean**: the minimum and maximum measurement are dropped, and the remaining 8 values are averaged.

This approach is standard in performance benchmarking. It eliminates the effect of transient outliers — such as an OS scheduler preempting the process mid-transfer or a garbage collection pause — without discarding as many data points as a more aggressive trim would. The result is a stable central estimate that is robust to the kind of one-off spikes observed during testing (e.g. a single HTTP batch run taking 6× longer than the others with identical payload).

The **coefficient of variation** (`cv_pct = std / mean × 100`) is also reported per configuration. Values below 10% indicate stable measurements. Values above 20% indicate that the result should be interpreted with caution and the raw runs should be inspected.

### Metrics recorded

**Raw file** (`benchmark_TIMESTAMP.csv`) — one row per individual run:

| Column | Description |
|---|---|
| `protocol` | `arrow` or `http` |
| `dataset` | source CSV filename |
| `num_rows` | total rows in the dataset |
| `chunk_size` | rows per batch |
| `num_batches` | total batches sent (`ceil(num_rows / chunk_size)`) |
| `file_size_mb` | size of the source CSV file in MB |
| `run` | run index (1 to N_RUNS) |
| `elapsed_ms` | total transfer time in milliseconds |
| `throughput_mbs` | effective throughput in MB/s (`file_size_mb / elapsed_s`) |

**Summary file** (`summary_TIMESTAMP.csv`) — one row per configuration:

| Column | Description |
|---|---|
| `mean_ms` | arithmetic mean of all runs |
| `median_ms` | median of all runs |
| `trimmed_mean_ms` | mean after dropping min and max — **primary metric** |
| `std_ms` | standard deviation |
| `cv_pct` | coefficient of variation in % (std / mean × 100) |
| `throughput_trimmed_mbs` | throughput derived from trimmed mean |

### What is and is not included in elapsed time

**Included:** opening the transport connection for data transfer, serializing and sending all batches, waiting for the server's final confirmation that all data was received.

**Excluded:** reading the CSV from disk (done once before all runs), the initial metadata handshake (identical overhead for both protocols).

---

## Configuration

`config.properties`:

```ini
[SETTINGS]
chunk_size = 1000
host = localhost
arrow_port = 3000
http_port = 8080

[VARIABLES]
data_file_path = ../resources/data_csv_1m.csv
logs_active = true
logs_dir = logs
```

`chunk_size` and `data_file_path` are overridden programmatically by `main.py` during experiments and only used as defaults when running clients directly.

---

## Dependencies

| `pyarrow`  | 15.0.2 | Arrow IPC format, Flight gRPC client and server 
| `pandas`   | 2.2.1  | CSV-to-DataFrame conversion for Arrow table construction 
| `requests` | 2.31.0 | HTTP client with session support 
