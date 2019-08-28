# Vulcan Agent

Vulcan Agent is the Vulcan component that runs and monitors check jobs.

Check jobs are received from a queue that implements the queue interface
defined by the `queue` package. Checks received from the queue are started and
monitored by the scheduler, found in the `scheduler` package. Checks consist on
container images which run on an agent runtime that implements the agent
interface defined on the `agent` package. State for the agent and the checks is
kept on the [persistence](https://github.com/adevinta/vulcan-persistence) service
with the help of the `persistence` package. In order to receive push commands
(such as "abort", "disconnect"...), the agent connects to a websocket
[stream](https://github.com/adevinta/vulcan-stream), from where it
receives commands that trigger actions in the agent, this relationship is
implemented on the `stream` package.

### Integrations

Agent Runtimes

- [x] Docker
- [x] Kubernetes

Queues

- [x] AWS SQS

### Disconnection

Disconnection is the procedure the agent starts when...

- ... a disconnect event is received from the stream.
- ... the agent becomes unable to kill a check.
- ... the agent loses connection to the stream.
- ... the agent loses connection to the persistence.

Disconnection is triggered by cancelling a
[context](https://golang.org/pkg/context/) which is passed to every component
within the agent and will trigger disconnection from the queue, abortion (or
killing, if non-responsive) of running jobs, persistence of the resulting state
and, ultimately, the termination of the agent process.

The main goal of the disconnection is to ensure that an agent that is not able
to respond to commands, not able to persist statuses and results, not able to
control its own checks, outated or unresponsive, is gracefully terminated so
that a new agent can take its place.

## Agent Components

### Agent

The agent is the core component which implements the agent interface for a
particular runtime environment. It defines how the agent will start, abort,
kill and retrieve the status and result of checks that it owns. It also exposes
functions to set and retrieve the status of the agent itself.

### Scheduler

The scheduler is the component which handles periodic tasks. It processes jobs
received by the queue, validates them and passes them to the agent for running.
It is in charge of updating the agent heartbeat. It also monitors the status
and progress for each check and updates it on the persistence.

### Queue

The queue is the component which handles receiving messages from a queue and
passing them to the scheduler for processing.

### Stream

The stream is the component which handles the processing of messages received
from the stream. It validates the content of the message, determines if the
message is directed to the agent, translates the action in the message to a
function and runs it. It keeps track of the state of the connection and handles
reconnecting to the stream if necessary.

### Persistence

The persistence is the component which implements functions to communicate with
the persistence service.
