-- DistriLab – CS324 Assignment 1
An unstructured distributed system of worker nodes that elect a coordinator and process jobs in parallel, using Java RMI.

*Requirements
- JDK 21
- Apache NetBeans (Ant project)

*Build
Open the project in NetBeans → right-click "DistriLab" → "Clean and Build".
This creates `dist\DistriLab.jar`.

*Run (quick demo)
From the project folder in Command Prompt: run-demo.bat
Enter the number of workers (3–10). The script starts the Bootstrap Node, the workers and 2 clients.
About 10 seconds after the workers start, the first leader election runs automatically.

*Run manually
Start each component in its own Command Prompt, in this order:
1. Bootstrap Node
   java -cp dist\DistriLab.jar bootstrap.BootstrapNode [port] [host]
2. Each worker (3–10)
   java -cp dist\DistriLab.jar worker.WorkerNode [bootstrapHost] [bootstrapPort] [thisHost]
3. Each client (2 or more)
   java -cp dist\DistriLab.jar client.JobClientGUI

Defaults: host `localhost`, port `1099`.

*Running across several computers
Use the Bootstrap computer's IP address as `bootstrapHost`, and pass each computer's own IP as `thisHost`.
In the client, enter the Bootstrap computer's IP and port.

*Worker console commands
- `e` – manually start a leader election
- `s` – show current coordinator and this worker's JAC
- `n` – show neighbours
- `w` – show all active workers
- `q` – leave the network

*Project structure
| Package | Classes | Responsibility |
|---|---|---|
| `common` | `WorkerService`, `BootstrapService`, `Job`, `JobResult`, `ElectionCandidate`, `WorkerInfo`, `Config`, `Log` | Shared RMI interfaces and data |
| `bootstrap` | `BootstrapNode` | Registers workers, assigns IDs, tracks active workers |
| `worker` | `WorkerNode`, `ElectionManager`, `JobCoordinator` | Networking, leader election, job coordination |
| `jobs` | `NumberListJob`, `MaxJob`, `PrimeCountJob`, `PrimeSumJob`, `TaskExecutor`, `PrimeUtils` | Job types and concurrent execution |
| `client` | `JobClientGUI`, `CsvLoader` | Swing client with manual and CSV input |

*Network
- The Bootstrap Node assigns each worker a unique integer ID and keeps a list of active workers.
- A joining worker is connected to one randomly chosen active worker, forming an unstructured network.
- Workers ping their neighbours every 5 seconds and reconnect if a neighbour fails.

*Leader election
- If there is no active coordinator, a worker starts an election automatically.
- The ELECTION message is passed from neighbour to neighbour. Each worker records the election ID and ignores duplicates.
- Each worker returns the best candidate from its part of the network: **lowest JAC wins; on a tie, highest ID wins**.
- The winner is announced with a COORDINATOR message, passed neighbour to neighbour.
- A coordinator serves one term of **5 jobs**, then a new election starts automatically.

*Job Allocation Counter (JAC)
A worker's JAC increases each time it, as coordinator, assigns part of a job to another worker.
Example: Workers 1, 2 and 3 all start with JAC 0, so Worker 3 (highest ID) becomes coordinator.
After its 5-job term its JAC is above 0, so in the next election Worker 2 (JAC 0, next highest ID) wins.

*Jobs
- MAX(numbers) – largest value in a list
- PRIMESUM(start, end)– sum of primes in the range
- PRIMECOUNT(numbers) – number of primes in a list

Every job type extends the abstract `Job` class and implements `split`, `execute` and `combine`.
The coordinator splits each job as evenly as possible across all active workers, sends the parts in parallel,
and combines the results. Each worker runs its parts on a thread pool, so it can process several jobs at once.

*Client
- Enter numbers manually (comma-separated) or load a CSV file. For PRIMESUM, the CSV's first two values are used as start and end.
- Multiple jobs can be submitted without waiting; each is labelled `[Job N]` in the results log.
- Multiple clients can run at the same time.
- If a job is submitted while the coordinator is changing, wait a few seconds and submit it again.

