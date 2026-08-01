# Eduardo-Rivera-esp32-avionics-rtos-capstone
# Dual-Core Avionics Telemetry and Fault-Monitoring System

A dual-core ESP32-S3 and FreeRTOS simulation that processes simulated
avionics data, monitors real-time system health, and demonstrates
controlled degradation and recovery during an injected fault.

> This is an educational simulation inspired by avionics timing and
> fault-management practices. It is not a certified airborne system.

## Project Overview

The system implements an avionics attitude-processing pipeline using
FreeRTOS on an ESP32-S3.

Core 1 runs the real-time processing tasks:

- Attitude Sensor
- Sensor Fusion
- Telemetry Coordinator
- Flight Responder

Core 0 runs the Wi-Fi and HTTP monitoring system.

The project demonstrates:

- FreeRTOS queues
- Event groups
- Direct task notifications
- GPIO interrupts
- Mutex-protected shared state
- Task timing measurements
- Queue back-pressure
- Fault injection
- Graceful recovery

## Live Dashboard

The HTTP dashboard displays:

- Fused roll and altitude
- Queue depth
- Queue high-water mark
- Dropped samples
- Task heartbeats
- Measured maximum task times
- Fault state
- System state

![Dashboard overview](assets/dashboard-overview.png)

## Fault-Injection Demonstration

The system includes a simulated 300 ms Sensor Fusion processing stall.

During the fault:

1. The producer continues generating data.
2. The queue fills to four items.
3. New samples begin to drop.
4. The system enters `DEGRADED` mode.

After the fault is removed:

1. The consumer drains the queue.
2. The system returns to `NOMINAL`.
3. Historical fault evidence remains visible.

## System Architecture

![System architecture](assets/architecture-diagram.png)

## Project Links

- **Wokwi simulation:** Add URL here
- **Demo video:** Add URL here
- **GitHub Pages site:** Add URL here

## Author

Eduardo Rivera  
Electrical Engineering Student  
University of Central Florida

## License

See [LICENSE](LICENSE).
