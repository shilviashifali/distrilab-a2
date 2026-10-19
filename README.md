# DistriLab – CS324 Assignment 2

A distributed computing cluster of worker nodes that elect a coordinator and run
MAX, PRIMESUM and PRIMECOUNT jobs in parallel. All communication uses **gRPC and
Protocol Buffers**. The system tolerates worker and coordinator failures.

## Requirements
- JDK 21
- Apache NetBeans (opens the Maven project directly) or Maven 3.9+
- Internet access on the first build (Maven downloads gRPC, Protobuf and `protoc`)

## Project structure
One NetBeans project (same layout as A1). Each program is its own package with a `main` class:

| Folder / package | Program / role | Main class |
|---|---|---|
| `src/bootstrap` | Bootstrap Node: assigns worker IDs, tracks active workers, removes failed ones, records the coordinator | `bootstrap.BootstrapNode` |
| `src/worker` | Worker Node: neighbour network, leader election, task execution, coordinator role, failure recovery | `worker.WorkerNode` |
| `src/client` | Client GUI: submit jobs, live progress bars, worker-level breakdown | `client.JobClientGUI` |
| `src/common` | Shared helpers used by all three: `Settings`, `Channels`, `Log`, `Defaults` | - |
| `proto/distrilab.proto` | The gRPC contract: every message and service | - |
| `demo-data/` | Sample CSV inputs for the client | - |

The project is built with Maven because gRPC is an external library: Maven downloads it and
generates the Java classes for the proto (package `grpc`) into `target/generated-sources`.
Generated code is not committed.

## 1. Build
In NetBeans: **File > Open Project** > select this folder > right-click the project > **Clean and Build**.
(Or from a terminal: `mvn clean package`.)

This produces one runnable jar: `target/distrilab.jar`.

## 2. Configure
Nothing is hard-coded. Every setting can be given as `--key=value` on the command line,
or in a `distrilab.properties` file in the folder you run from. Command-line values win.

| Setting | Used by | Default | Meaning |
|---|---|---|---|
| `port` | bootstrap, worker | 50050 / any free port | Port this program listens on |
| `host` | worker | localhost | Address other nodes use to reach this worker |
| `bootstrapHost` | worker, client | localhost | Bootstrap Node address |
| `bootstrapPort` | worker, client | 50050 | Bootstrap Node port |
| `neighbours` | worker | 2 | Neighbours a joining worker connects to |
| `pingInterval` | all | 2 | Seconds between liveness checks |
| `missedPings` | all | 3 | Missed checks in a row before a node counts as failed |
| `rpcTimeoutMs` | all | 2000 | Deadline for short gRPC calls |

Example `distrilab.properties`:
```
bootstrapHost=192.168.1.10
bootstrapPort=50050
```

## 3. Run
Start each program in its own window, in this order. On Windows, double-click the scripts:

1. **Bootstrap Node** (one) - `run-bootstrap.bat`
2. **Workers** (3-10; more can join at any time, even while jobs run) - `run-worker.bat`, once per worker.
   The first election starts automatically a few seconds after workers join.
3. **Clients** (any number - each one is a separate program with its own window) - `run-client.bat`, once per client.

`run-demo.bat` asks how many workers and clients to start and opens them all.

Equivalent commands (any OS):
```
java -cp target/distrilab.jar bootstrap.BootstrapNode --port=50050
java -cp target/distrilab.jar worker.WorkerNode --bootstrapHost=localhost --bootstrapPort=50050
java -cp target/distrilab.jar client.JobClientGUI --bootstrapHost=localhost --bootstrapPort=50050
```
Extra `--key=value` settings can be added after the script name, e.g. `run-worker.bat --bootstrapHost=192.168.1.10`.

### Running across several computers
On each worker computer pass its own LAN address so others can reach it:
`--host=<this computer's IP> --bootstrapHost=<bootstrap IP>`. Allow the ports through the firewall.

## 4. Demo: failure tolerance
- **Worker failure:** submit a large PRIMESUM job (e.g. 1 to 20,000,000), then close a worker window.
  The coordinator detects it, reassigns that worker's unfinished task, and the progress bar keeps going.
- **New worker mid-job:** start another worker while a job runs; it is used for the next tasks.
- **Coordinator failure:** close the coordinator's window while a job runs. The workers detect it,
  elect a new coordinator, which continues the job from the replicated state. The client reconnects
  automatically and its progress bar resumes.

## Worker console commands
- `e` – start a leader election
- `s` – show coordinator, term and this worker's JAC
- `n` – show neighbours
- `w` – show all active workers
- `q` – leave the network
