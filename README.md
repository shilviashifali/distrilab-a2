# DistriLab – CS324 Assignment 2

A distributed computing cluster of worker nodes that elect a coordinator and run
MAX, PRIMESUM and PRIMECOUNT jobs in parallel. All communication uses **gRPC and
Protocol Buffers**. The system tolerates worker and coordinator failures.

## Requirements
- JDK 21
- Apache NetBeans (opens the Maven project directly) or Maven 3.9+
- Internet access on the first build (Maven downloads gRPC, Protobuf and `protoc`)

## Project structure
One repository and one build, with a **separate project for each program** (Maven multi-module):

| Project (folder) | What it is | Main class / output |
|---|---|---|
| `common/` | Shared library: the gRPC contract `proto/distrilab.proto` and helpers `Settings`, `Channels`, `Log`, `Defaults` | library |
| `bootstrap/` | **Bootstrap Node**: assigns worker IDs, tracks active workers, removes failed ones, records the coordinator | `bootstrap.BootstrapNode` / `bootstrap/target/bootstrap.jar` |
| `worker/` | **Worker Node**: neighbour network, leader election, task execution (`jobs` package: MAX, PRIMESUM, PRIMECOUNT), coordinator role, failure recovery | `worker.WorkerNode` / `worker/target/worker.jar` |
| `client/` | **Client GUI**: one progress bar per job, click a bar for the worker breakdown, CSV input, log | `client.JobClientGUI` / `client/target/client.jar` |
| `pom.xml` | Parent project that builds the four together | - |

The three programs depend only on `common`, never on each other, so the proto is defined once and shared.
Maven generates the Java classes for the proto (package `grpc`) into `common/target/generated-sources`;
generated code is not committed.

## 1. Build
In NetBeans: **File > Open Project** > select this folder, tick **Open Required Projects** > **Open Project**.
This opens the parent and the four projects. Right-click **DistriLab** (the parent) > **Clean and Build**.
(Or from a terminal: `mvn clean install`.)

This produces one runnable jar per program: `bootstrap/target/bootstrap.jar`, `worker/target/worker.jar`, `client/target/client.jar`.

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
| `tasksPerWorker` | worker | 3 | A job is split into (active workers x this) tasks |
| `slowAfterSeconds` | worker | 6 | A task with no progress for this long is checked: slow (still answers pings) or failed |
| `workerThreads` | worker | 4 | Tasks one worker can compute at the same time |

Example `distrilab.properties`:
```
bootstrapHost=192.168.1.10
bootstrapPort=50050
```

## 3. Run
Start each program separately, in this order.

**In NetBeans:** in the program's project, open the class in **Source Packages**, right-click > **Run File** (Shift+F6).
Each run is a separate program with its own Output tab; run a class again to start another copy.

1. **Bootstrap Node** (one): project *DistriLab Bootstrap Node* > `bootstrap/BootstrapNode.java`
2. **Workers** (3-10; more can join at any time, even while jobs run): project *DistriLab Worker Node* > `worker/WorkerNode.java`, once per worker.
   The first election starts automatically a few seconds after workers join.
3. **Clients** (any number, each with its own window): project *DistriLab Client* > `client/JobClientGUI.java`, once per client.

To stop a program, click the red stop button on its Output tab.

**From a terminal** (after building), one terminal per program:
```
java -jar bootstrap/target/bootstrap.jar --port=50050
java -jar worker/target/worker.jar --bootstrapHost=localhost --bootstrapPort=50050
java -jar client/target/client.jar --bootstrapHost=localhost --bootstrapPort=50050
```

### Running across several computers
On each worker computer pass its own LAN address so others can reach it:
`--host=<this computer's IP> --bootstrapHost=<bootstrap IP>`. Allow the ports through the firewall.

## 4. Demo: failure tolerance
- **Worker failure:** submit a large PRIMESUM job (e.g. 1 to 20,000,000), then stop a worker (red stop button on its Output tab).
  The coordinator detects it, reassigns that worker's unfinished task, and the progress bar keeps going.
- **New worker mid-job:** start another worker while a job runs; it is used for the next tasks.
- **Coordinator failure:** stop the coordinator worker while a job runs. The workers detect it,
  elect a new coordinator, which continues the job from the replicated state. The client reconnects
  automatically and its progress bar resumes.
## 5. Client and Real-Time Job Progress

The client (`client.JobClientGUI`) submits jobs to the coordinator and follows each one through the
**WatchJob gRPC server stream**. Run it once per client; every window gets its own client ID.

- **Any number of jobs:** each submitted job gets its own row with its own progress bar.
- **Real progress:** the percentage comes from completed work reported by the workers, not a timer.
- **Worker breakdown:** click a job's progress bar to open a live table of its tasks - which worker runs each one,
  its status (RUNNING, SLOW, REASSIGNED, WAITING, DONE) and items done.
- **Failover:** if the coordinator fails, the client asks the Bootstrap Node for the new coordinator and calls
  WatchJob again; while an election or a term change is in progress, submission is retried automatically.
  Each job has a client-generated UUID, so a retried submission is never run twice.
- **Input:** type numbers or a range, or use **Load CSV**; invalid input is reported in a dialog.
- **Log:** timestamped events for every job, with a **Clear Log** button.
- **Configuration:** `--bootstrapHost` / `--bootstrapPort` (or `distrilab.properties`), default `localhost:50050`.

| Class | Responsibility |
|---|---|
| `client.JobClientGUI` | Window, input and CSV loading, log, connections to Bootstrap and coordinator |
| `client.JobPanel` | One job: submit with retry, follow the progress stream, reconnect after coordinator failure |
| `client.BreakdownDialog` | Worker-level breakdown table for one job |

## Worker console commands
- `e` – start a leader election
- `s` – show coordinator, term and this worker's JAC
- `n` – show neighbours
- `w` – show all active workers
- `q` – leave the network
- `job PRIMESUM 1 20000000`, `job MAX 4,8,15,16`, `job PRIMECOUNT 2,3,4,5,7` – submit a test job with this worker as coordinator; progress is printed every 10%
