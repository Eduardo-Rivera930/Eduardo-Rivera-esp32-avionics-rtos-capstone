# *App 5: Avionics Dual-Core IPC Pipeline*

## System architecture
  On Core 1, the Attitude Sensor task acts as the producer. It generates a simulated `attitude_sample_t` containing gyroscope roll, accelerometer roll, altitude, a timestamp, and a sample ID. The producer sends each sample to `data_q` using `xQueueSend()`.

  The Sensor Fusion task receives samples using `xQueueReceive()` and combines the gyroscope and accelerometer measurements into a fused roll estimate. The producer and consumer set separate bits in `evt_group`. The Telemetry Coordinator waits until both the produced and processed bits are set using `xEventGroupWaitBits()`.

  After the rendezvous completes, the coordinator sends a direct task notification to the Flight Responder. A GPIO 18 button can also notify the responder through the GPIO ISR using `vTaskNotifyGiveFromISR()`.

  On Core 0, the Wi-Fi and HTTP server display the live queue depth, event-group state, last processed sample, and task heartbeat counters.

## Queue contract

  - **Producer:** Attitude Sensor task
  - **Consumer:** Sensor Fusion task
  - **Item type:** `attitude_sample_t`
  - **Producer rate:** 20 Hz, or one sample every 50 ms
  - **Queue depth:** 4 items
  - **Send timeout:** 10 ms
  - **Receive timeout:** 200 ms
  - **Back-pressure policy:** Wait up to 10 ms, then log and drop the newest sample
  - **Item size:** 16 bytes, as reported by `sizeof(attitude_sample_t)`
  - **Queue payload capacity:** \(4 \times 16 = 64\) bytes, excluding FreeRTOS queue-control overhead
## Engineering Analysis 
### 1. Why the web server is on Core 0
  The web server is pinned to Core 0 to separate the observability workload from the real-time pipeline on Core 1. Wi-Fi and HTTP processing can have variable execution times, so placing the server on Core 1 could delay the producer, consumer, coordinator, or responder and increase scheduling jitter. It could also allow the queue to build up or increase notification latency. Placing the server on Core 0 reduces direct CPU scheduling interference with the pipeline, although both cores still share memory and other hardware resources.  
### 2. How the queue depth was selected
  The producer generates one attitude sample every 50 ms. I selected a 200 ms burst-protection window, during which the producer could generate: N(Burst) = 200 ms / 50 ms = 4. Therefore, I selected a queue depth of four samples. This allows the queue to absorb four arrivals while the consumer is temporarily unavailable. When the queue is full, the producer waits up to 10 ms for space. If space does not become available, it logs the condition and drops the newest sample rather than blocking indefinitely.
### 3. Event group versus multiple semaphores
  An event group is a better fit for this rendezvous because several conditions can be represented as separate bits inside one event-group object. The coordinator can use xEventGroupWaitBits() to wait until all required stages have completed. With separate semaphores, one semaphore would be needed for each condition, and the coordinator would have to take and manage them individually. Event groups also scale conveniently because another condition can be represented by another bit. However, event bits only represent Boolean conditions; repeatedly setting an already-set bit does not preserve the number of occurrences. Semaphores are more appropriate when individual events must be counted or when access to a resource is being controlled.
### 4. Direct notification versus binary semaphore
  A direct task notification is the better fit for the button-to-responder path because there is exactly one sender event and one receiving task. The notification value is stored directly in the receiving task’s control block, so no separate semaphore object is required. A binary semaphore is more flexible because different tasks or interrupts can give it and a task can wait on the separate synchronization object, but this introduces additional kernel-object overhead. In my 50-press test, the direct notification had a minimum latency of 25 µs, an average of 33 µs, and a maximum of 34 µs. The binary semaphore had a minimum of 72 µs, an average of 129 µs, and a maximum of 1,682 µs. Therefore, the direct notification was faster and more consistent in this experiment. 

## Latency Measurements
  ![Calculations](https://github.com/Eduardo-Rivera930/RTS-Sum2026/raw/main/Notif%20and%20Sem%20Calculations)
  | Mechanism                | Samples | Minimum | Average |  Maximum |
  | ------------------------ | ------: | ------: | ------: | -------: |
  | Direct task notification |      50 |   25 µs |   33 µs |    34 µs |
  | Binary semaphore         |      50 |   72 µs |  129 µs | 1,682 µs |

  Wakeup latency was measured over 50 accepted button presses. Each ISR activation signaled both a direct task notification and a binary semaphore under the same conditions. The direct notification had a minimum latency of 25 µs, an average of 33 µs, and a maximum of 34 µs. The binary semaphore had a minimum latency of 72 µs, an average of 129 µs, and a maximum of 1,682 µs. In this experiment, the direct notification was faster and considerably more consistent. This result is reasonable because a task notification is stored directly in the target task’s control block, while a semaphore uses a separate kernel object and provides more general synchronization behavior.
## Back-pressure test
  ![Queue full](https://github.com/Eduardo-Rivera930/RTS-Sum2026/raw/main/Queue%20Full)

  The queue depth is four because the producer can generate four samples during the selected 200 ms burst-protection window. The producer waits up to 10 ms for space. If the queue remains full, it logs the condition and drops the newest sample so it does not block indefinitely. For testing, the consumer was temporarily delayed so that the 20 Hz producer could fill the four-item queue. The artificial delay was removed after the queue-full behavior was verified.

## Web monitor
  The HTTP monitor runs on Core 0 and refreshes once per second. It displays the
  queue depth, event bits, most recently processed sample, fused roll, altitude,
  and all four task heartbeat counters.

  ![Avionics web monitor](https://github.com/Eduardo-Rivera930/RTS-Sum2026/raw/main/App%205%20Web%20monitor)

## Concurrency diagram
  ![Concurrency Diagram](https://github.com/Eduardo-Rivera930/RTS-Sum2026/raw/main/App%205%20CC%20diagram)

## Reused-code citations
  - Wi-Fi initialization and HTTP server patterns were adapted from App 1.
  - ISR-to-task latency measurement logic was adapted from App 3.
  - Heartbeat counters, IPC object wiring, serial-monitor structure, and task
    skeletons were provided by the App 5 scaffold.
  - Producer logic, consumer processing, queue policy, web-page content,
    engineering analysis, and avionics theming were completed for App 5.


## Worst Case Execution Time (WCET) Table
| Task                  | Activation             | Core | Priority | Function                            |         Nominal max |      Worst observed |
| --------------------- | ---------------------- | ---: | -------: | ----------------------------------- | ------------------: | ------------------: |
| Attitude Sensor       | 50 ms periodic         |    1 |        8 | Generate and queue samples          |               89 µs |           12,850 µs |
| Sensor Fusion         | Queue arrival          |    1 |        8 | Combine gyro and accelerometer data |            1,650 µs |            1,650 µs |
| Telemetry Coordinator | Event-group rendezvous |    1 |        9 | Detect completed pipeline cycle     |              803 µs |              803 µs |
| Flight Responder      | Task notification      |    1 |       12 | Respond to cycle completion or ISR  |              203 µs |              203 µs |
| HTTP Server           | HTTP request / polling |    0 |        5 | Serve dashboard and fault controls  | Measure or mark N/A | Measure or mark N/A |

The 12,850 µs maximum occurred during fault injection and includes the producer waiting up to 10 ms for queue space before applying the drop-newest policy. Therefore, it represents maximum measured task-body duration under back-pressure rather than computation-only WCET.

## Hazard Analysis

| Hazard                                | System effect                                              | Detection                                          | Mitigation                                                        |
| ------------------------------------- | ---------------------------------------------------------- | -------------------------------------------------- | ----------------------------------------------------------------- |
| Sensor-fusion processing stall        | Queue fills and telemetry becomes stale                    | Queue depth, high-water mark, heartbeat divergence | Enter degraded mode and drop newest samples                       |
| Queue overflow                        | New attitude samples are lost                              | Failed `xQueueSend()` and drop counter             | Wait 10 ms, log, then drop newest                                 |
| Consumer stops running                | Producer heartbeat advances while consumer heartbeat stops | Heartbeat comparison                               | Report degraded state; future production version could reset task |
| Button bounce                         | Multiple false ISR events                                  | Unexpectedly high press count                      | 200 ms ISR debounce                                               |
| Invalid task handle                   | Assertion or reset during notification                     | Task-creation return checks                        | Create responder first and verify its handle                      |
| Inconsistent dashboard snapshot       | Fields from different pipeline cycles appear together      | Inconsistent status display                        | Protect `system_status_t` with a mutex                            |
| Web workload interferes with pipeline | Increased scheduling jitter                                | Timing measurements                                | Pin HTTP server to Core 0                                         |
