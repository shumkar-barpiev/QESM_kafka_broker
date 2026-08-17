# Kafka Batching STPN Model

This project studies a simplified Kafka-style batching system. It was created for the course **Quantitative Evaluation of Stochastic Models**.

The model helps us study a basic trade-off:

- A larger batch can send several messages together.
- Waiting for a batch can increase message delay.
- A timeout prevents messages from waiting for too long.

This is a study model. It does not reproduce the complete Kafka system.

## 1. Tools

The project uses:

- **ORIS Tool** to draw and check the first STPN model;
- **Sirio 2.0.5** to analyse the model in Java;
- **Maven** to build and run the project;
- **Java 24** as the target Java version.

## 2. Model idea

Messages arrive one at a time. Each message is represented by one token in `P_Buffer`.

The current maximum batch size is:

$$
B=3
$$

A batch is dispatched when one of these events happens:

1. The buffer reaches three messages.
2. The timeout expires while one or two messages are waiting.

The timeout is deterministic. Arrivals and service times are exponential. Because the model contains a deterministic transition, it is an **STPN**, not a pure CTMC.

The main flow is:

```text
Arrival
  -> Buffer
  -> Full batch or timeout
  -> Batch service
  -> Broker becomes idle again
```

## 3. Main places

| Place | Meaning |
|---|---|
| `P_Buffer` | Messages waiting for dispatch |
| `P_TimerActive` | The timeout is running |
| `P_TimeoutExpired` | The timeout has expired |
| `P_Timeroff` | The timeout is not running |
| `P_BatchService` | Messages in the batch being sent |
| `P_BrokerIdle` | The broker is ready to send a batch |
| `P_BrokerBusy` | The broker is sending a batch |

## 4. Main transitions

| Transition | Type | Meaning |
|---|---|---|
| `ArrivalAccepted` | Exponential | Adds one message to the buffer |
| `T_StartTimeout` | Immediate | Starts the timer after the first waiting message |
| `T_Timeout` | Deterministic | Marks the timeout as expired |
| `T_DispatchPartial_1` | Immediate | Dispatches one message after timeout |
| `T_DispatchPartial_2` | Immediate | Dispatches two messages after timeout |
| `T_DispatchFullActive` | Immediate | Dispatches three messages while the timer is active |
| `T_DispatchFullExpired` | Immediate | Dispatches three messages when timeout and full-batch events meet |
| `T_Send1` | Exponential | Completes a one-message batch |
| `T_Send2` | Exponential | Completes a two-message batch |
| `T_Send3` | Exponential | Completes a three-message batch |

Weighted arcs and inhibitor arcs are added in Java. They make sure that every dispatch and send transition uses the correct number of messages.

## 5. Initial marking

The model starts with:

- an empty buffer;
- an idle broker;
- no batch in service;
- the timer turned off;
- no expired timeout.

## 6. Parameters

The model has these parameters:

| Parameter | Meaning |
|---|---|
| $\lambda$ | Message arrival rate |
| $T$ | Deterministic timeout duration |
| $B$ | Maximum batch size, currently fixed at 3 |
| $\mu_1$ | Service rate for a one-message batch |
| $\mu_2$ | Service rate for a two-message batch |
| $\mu_3$ | Service rate for a three-message batch |

The default values are all equal to `1`. Analysis classes can give different values to the model.

## 7. Project structure

```text
qesm/
├── pom.xml
├── README.md
├── draw_plots.ipynb
├── outcomes/
│   ├── steady-results.csv
│   └── transient-results.csv
└── src/
    ├── main/java/com/myexam/qesm/
    │   ├── kafka/
    │   │   └── KafkaBrokerModel.java
    │   └── analysis/
    │       ├── RegenerativeSteadyState.java
    │       └── RegenerativeTransient.java
    └── test/java/com/myexam/qesm/
        └── AppTest.java
```

## 8. Build and test

Open a terminal in the folder that contains `pom.xml`.

Build and test the project:

```bash
mvn test
```

The command should finish with:

```text
BUILD SUCCESS
```

## 9. Regenerative steady-state analysis

Regenerative steady-state analysis studies the long-run behaviour of the bounded STPN model. It does not focus on the first few seconds after startup.

The current experiment uses:

$$
\lambda \in \{0.25, 0.5, 1, 2\}
$$

$$
T \in \{1, 3, 5\}
$$

It keeps $B=3$ and $\mu_1=\mu_2=\mu_3=1$.

Run it with:

```bash
mvn -q org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.mainClass=com.myexam.qesm.analysis.RegenerativeSteadyState
```

Save the output:

```bash
mkdir -p outcomes

mvn -q org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.mainClass=com.myexam.qesm.analysis.RegenerativeSteadyState \
  > outcomes/steady-results.csv
```

### Steady-state metrics

| Metric | Meaning |
|---|---|
| `probability_sum` | Sum of all steady-state probabilities; it should be close to 1 |
| `effective_arrival_rate` | Rate of messages accepted by the finite buffer |
| `average_buffer` | Average number of waiting messages |
| `batch_throughput` | Completed batches per time unit |
| `message_throughput` | Completed messages per time unit |
| `average_batch_size` | Average messages in a completed batch |
| `queue_waiting_time` | Average time before batch dispatch |
| `total_system_time` | Average time before dispatch plus service time |

Average queue waiting time is calculated with Little's Law:

$$
E[W_q] = \frac{E[N_{buffer}]}{\lambda_{effective}}
$$

The effective arrival rate is used because the model stops accepting arrivals when the buffer already contains three messages.

### Expected steady-state behaviour

- A longer timeout usually produces larger batches.
- A longer timeout can increase waiting time, mainly under low traffic.
- Under high traffic, the buffer reaches three messages more often.
- Message throughput should be close to the effective arrival rate in a stable model.

## 10. Regenerative transient analysis

Regenerative transient analysis studies how the bounded STPN model changes over time after it starts from an empty state.

The current experiment uses:

$$
\lambda \in \{0.25, 1, 2\}, \qquad T=3
$$

The analysis period is:

$$
0 \leq t \leq 15, \qquad \Delta t=0.5
$$

Run it with:

```bash
mvn -q org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.mainClass=com.myexam.qesm.analysis.RegenerativeTransient
```

Save the output:

```bash
mkdir -p outcomes

mvn -q org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.mainClass=com.myexam.qesm.analysis.RegenerativeTransient \
  > outcomes/transient-results.csv
```

### Transient metrics

| Metric | Meaning |
|---|---|
| `p_buffer_0` to `p_buffer_3` | Probability of each buffer size at time $t$ |
| `p_broker_busy` | Probability that the broker is sending at time $t$ |
| `p_timer_active` | Probability that the timer is active at time $t$ |
| `average_buffer` | Expected waiting messages at time $t$ |
| `average_in_service` | Expected messages being sent at time $t$ |
| `effective_arrival_rate` | Accepted arrival rate at time $t$ |
| `batch_completion_rate` | Batch completion rate at time $t$ |
| `message_completion_rate` | Message completion rate at time $t$ |
| `cumulative_batches` | Expected batches completed from time 0 to $t$ |
| `cumulative_messages` | Expected messages completed from time 0 to $t$ |

### Useful transient graphs

- Average buffer against time.
- Broker-busy probability against time.
- Message completion rate against time.
- Cumulative completed messages against time.
- Buffer-size probabilities as a stacked chart.

## 11. Validation checks

Use these checks before accepting the results:

1. `probability_sum` should be close to 1.
2. At time 0, `p_buffer_0` should equal 1.
3. A full dispatch must use exactly three messages.
4. A timeout must not send an empty batch.
5. `T_DispatchPartial_1` must use one message.
6. `T_DispatchPartial_2` must use two messages.
7. In steady state, message throughput should match the effective arrival rate.
8. Under low traffic, timeout dispatch should be more important.
9. Under high traffic, full-batch dispatch should be more important.

## 12. Current limitations

- The model uses a message-count limit, while real Kafka commonly defines batch size in bytes.
- The current model supports only $B=3$.
- The buffer can contain at most three waiting messages. An arrival is not accepted when it is full.
- The model has one broker and one batch service process.
- There are no Kafka partitions, replicas, acknowledgements, retries, or network failures.
- Producer and consumer behaviour is simplified.
- The three service rates are currently equal in the analyses.
- The timeout is deterministic, but real system timing can contain more variation.

These limits are intentional. The project studies the main relationship between traffic, timeout, batch size, throughput, and waiting time.

## 13. Next steps

Possible next steps are:

1. Generate the model for different values of $B$.
2. Define service time as a function of batch size.
3. Add stronger automatic model tests.
4. Plot the steady-state and transient CSV files.
5. Compare model results with measurements from a small Kafka test.
