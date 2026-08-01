/*
 * Final Capstone — Dual-Core Avionics Telemetry and Fault-Monitoring System
 *
 * Core 1: real-time avionics pipeline
 *   Attitude Sensor -> Queue -> Sensor Fusion -> Event Group ->
 *   Telemetry Coordinator -> Direct Task Notification -> Flight Responder
 *
 * Core 0: Wi-Fi / HTTP observability plane
 *
 * Portfolio additions beyond App 5:
 *   - Mutex-protected system-status snapshot
 *   - Queue drop counter and queue high-water mark
 *   - Measured maximum task-body execution times
 *   - Web-controlled sensor-fusion stall fault
 *   - NOMINAL / DEGRADED state and automatic recovery
 *
 * Run modes:
 *   USE_WEBSERVER = 0 -> serial monitor on Core 0
 *   USE_WEBSERVER = 1 -> HTTP dashboard on Core 0
 *
 * Test modes:
 *   LATENCY_TEST_MODE = 0 -> final integrated pipeline
 *   LATENCY_TEST_MODE = 1 -> button notification vs semaphore latency test
 */

#ifndef USE_WEBSERVER
#define USE_WEBSERVER 1
#endif

#ifndef LATENCY_TEST_MODE
#define LATENCY_TEST_MODE 0
#endif

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/event_groups.h"
#include "freertos/semphr.h"

#include "driver/gpio.h"
#include "esp_attr.h"
#include "esp_err.h"
#include "esp_log.h"
#include "esp_timer.h"

#if USE_WEBSERVER
#include "esp_event.h"
#include "esp_http_server.h"
#include "esp_netif.h"
#include "esp_wifi.h"
#include "nvs_flash.h"
#endif

/*
 * Wokwi logging workaround used in the earlier applications.
 * The #undef statements prevent macro-redefinition warnings.
 */
#ifdef CONFIG_LOG_DEFAULT_LEVEL
#undef CONFIG_LOG_DEFAULT_LEVEL
#endif
#define CONFIG_LOG_DEFAULT_LEVEL 1

#ifdef CONFIG_LOG_MAXIMUM_LEVEL
#undef CONFIG_LOG_MAXIMUM_LEVEL
#endif
#define CONFIG_LOG_MAXIMUM_LEVEL 5

#define BUTTON_GPIO              GPIO_NUM_18
#define DEBOUNCE_US              200000LL

#define DATA_Q_DEPTH             4U
#define PRODUCER_PERIOD_MS       50U
#define QUEUE_SEND_TIMEOUT_MS    10U
#define QUEUE_RECEIVE_TIMEOUT_MS 200U
#define FUSION_FAULT_DELAY_MS    300U

#if USE_WEBSERVER
#define WIFI_SSID "Wokwi-GUEST"
#define WIFI_PASS ""
#define HTTP_PORT 80
#endif

#define EV_BIT_DATA_PRODUCED  BIT0
#define EV_BIT_DATA_PROCESSED BIT1

static const char *TAG = "avionics_capstone";

/* -------------------------------------------------------------------------- */
/* Avionics data types                                                        */
/* -------------------------------------------------------------------------- */

typedef struct {
    uint32_t sample_id;
    uint32_t timestamp_ms;

    /* Values are stored in tenths of a degree. */
    int16_t gyro_roll_x10;
    int16_t accel_roll_x10;

    int16_t altitude_m;
} attitude_sample_t;

typedef struct {
    /* Latest processed avionics data. */
    uint32_t last_sample_id;
    int32_t fused_roll_x10;
    int32_t altitude_m;

    /* Queue-pressure and fault evidence. */
    uint32_t dropped_samples;
    uint32_t queue_high_watermark;

    /* Measured maximum task-body durations. */
    uint64_t producer_wcet_us;
    uint64_t consumer_wcet_us;
    uint64_t coordinator_wcet_us;
    uint64_t responder_wcet_us;

    /* Current operating condition. */
    bool fault_active;
    bool degraded;
} system_status_t;

/* -------------------------------------------------------------------------- */
/* IPC objects and shared state                                               */
/* -------------------------------------------------------------------------- */

static QueueHandle_t data_q;
static EventGroupHandle_t evt_group;
static TaskHandle_t responder_handle;

static SemaphoreHandle_t status_mutex;
static system_status_t system_status;

/* Heartbeats are read by the observability task as proof of life. */
static volatile uint32_t hb_prod;
static volatile uint32_t hb_cons;
static volatile uint32_t hb_coord;
static volatile uint32_t hb_resp;

/* Accepted button presses after debounce. */
static volatile uint32_t presses_observed;
static volatile int64_t last_edge_us;

#if LATENCY_TEST_MODE
static volatile int64_t isr_entry_time_us;
static SemaphoreHandle_t latency_sem;

static volatile uint32_t notif_samples;
static volatile uint32_t sem_samples;

static uint64_t notif_latency_min_us;
static uint64_t notif_latency_max_us;
static uint64_t notif_latency_sum_us;

static uint64_t sem_latency_min_us;
static uint64_t sem_latency_max_us;
static uint64_t sem_latency_sum_us;
#endif

#if USE_WEBSERVER
static httpd_handle_t server_handle;
#endif

/* -------------------------------------------------------------------------- */
/* Shared-status helpers                                                      */
/* -------------------------------------------------------------------------- */

static bool get_status_snapshot(system_status_t *snapshot)
{
    if (snapshot == NULL || status_mutex == NULL) {
        return false;
    }

    if (xSemaphoreTake(status_mutex, pdMS_TO_TICKS(20)) != pdTRUE) {
        return false;
    }

    *snapshot = system_status;
    xSemaphoreGive(status_mutex);
    return true;
}

static bool is_fusion_fault_active(void)
{
    bool active = false;

    if (status_mutex != NULL &&
        xSemaphoreTake(status_mutex, pdMS_TO_TICKS(10)) == pdTRUE) {
        active = system_status.fault_active;
        xSemaphoreGive(status_mutex);
    }

    return active;
}

/* -------------------------------------------------------------------------- */
/* Core-1 real-time tasks                                                     */
/* -------------------------------------------------------------------------- */

static void producer_task(void *arg)
{
    (void)arg;

    uint32_t sample_id = 0;
    TickType_t last_wake_time = xTaskGetTickCount();

    for (;;) {
        const int64_t task_start_us = esp_timer_get_time();
        attitude_sample_t sample;

        sample.sample_id = sample_id;
        sample.timestamp_ms = (uint32_t)(esp_timer_get_time() / 1000LL);

        /* Simulated roll changes gradually from -2.0 through +2.0 degrees. */
        const int32_t phase = (int32_t)(sample_id % 21U) - 10;
        sample.gyro_roll_x10 = (int16_t)(phase * 2);
        sample.accel_roll_x10 =
            (int16_t)(sample.gyro_roll_x10 +
                      ((int32_t)(sample_id % 3U) - 1));

        /* Simulated altitude varies between 10,000 and 10,049 meters. */
        sample.altitude_m = (int16_t)(10000 + (sample_id % 50U));

        const BaseType_t sent = xQueueSend(
            data_q,
            &sample,
            pdMS_TO_TICKS(QUEUE_SEND_TIMEOUT_MS)
        );

        const UBaseType_t current_depth = uxQueueMessagesWaiting(data_q);

        if (xSemaphoreTake(status_mutex, portMAX_DELAY) == pdTRUE) {
            if ((uint32_t)current_depth > system_status.queue_high_watermark) {
                system_status.queue_high_watermark = (uint32_t)current_depth;
            }

            if (sent != pdPASS) {
                system_status.dropped_samples++;
                system_status.degraded = true;
            }

            xSemaphoreGive(status_mutex);
        }

        if (sent == pdPASS) {
            ESP_LOGD(
                TAG,
                "[attitude sensor] queued sample=%lu gyro=%d accel=%d "
                "altitude=%d m depth=%u",
                (unsigned long)sample.sample_id,
                sample.gyro_roll_x10,
                sample.accel_roll_x10,
                sample.altitude_m,
                (unsigned)current_depth
            );

            xEventGroupSetBits(evt_group, EV_BIT_DATA_PRODUCED);
        } else {
            ESP_LOGW(
                TAG,
                "[attitude sensor] queue full — dropping newest sample=%lu",
                (unsigned long)sample.sample_id
            );
        }

        sample_id++;
        hb_prod++;

        /*
         * This is a measured maximum task-body duration. Because xQueueSend()
         * has a bounded timeout, the value may include time blocked for queue
         * space when the fault is active.
         */
        const uint64_t elapsed_us =
            (uint64_t)(esp_timer_get_time() - task_start_us);

        if (xSemaphoreTake(status_mutex, portMAX_DELAY) == pdTRUE) {
            if (elapsed_us > system_status.producer_wcet_us) {
                system_status.producer_wcet_us = elapsed_us;
            }
            xSemaphoreGive(status_mutex);
        }

        vTaskDelayUntil(
            &last_wake_time,
            pdMS_TO_TICKS(PRODUCER_PERIOD_MS)
        );
    }
}

static void consumer_task(void *arg)
{
    (void)arg;

    attitude_sample_t sample;

    for (;;) {
        const BaseType_t received = xQueueReceive(
            data_q,
            &sample,
            pdMS_TO_TICKS(QUEUE_RECEIVE_TIMEOUT_MS)
        );

        if (received != pdTRUE) {
            ESP_LOGW(
                TAG,
                "[sensor fusion] no attitude sample received within %u ms",
                (unsigned)QUEUE_RECEIVE_TIMEOUT_MS
            );
            hb_cons++;
            continue;
        }

        /*
         * Fault injection: simulate a sensor-fusion task that is unexpectedly
         * slow. The status mutex is intentionally NOT held during this delay.
         */
        if (is_fusion_fault_active()) {
            vTaskDelay(pdMS_TO_TICKS(FUSION_FAULT_DELAY_MS));
        }

        /* Measure real processing after the artificial fault delay. */
        const int64_t task_start_us = esp_timer_get_time();

        const int32_t fused_roll_x10 =
            (8 * sample.gyro_roll_x10 +
             2 * sample.accel_roll_x10) / 10;

        const char *flight_state =
            (fused_roll_x10 > 50 || fused_roll_x10 < -50)
                ? "BANK WARNING"
                : "STABLE";

        const UBaseType_t current_depth = uxQueueMessagesWaiting(data_q);

        /* Signal that this queue item has completed themed processing. */
        xEventGroupSetBits(evt_group, EV_BIT_DATA_PROCESSED);

        const uint64_t elapsed_us =
            (uint64_t)(esp_timer_get_time() - task_start_us);

        if (xSemaphoreTake(status_mutex, portMAX_DELAY) == pdTRUE) {
            system_status.last_sample_id = sample.sample_id;
            system_status.fused_roll_x10 = fused_roll_x10;
            system_status.altitude_m = sample.altitude_m;

            if (elapsed_us > system_status.consumer_wcet_us) {
                system_status.consumer_wcet_us = elapsed_us;
            }

            /*
             * Recovery rule: after the injected fault is disabled, return to
             * NOMINAL once the queue has mostly drained.
             */
            if (!system_status.fault_active && current_depth <= 1U) {
                system_status.degraded = false;
            }

            xSemaphoreGive(status_mutex);
        }

        ESP_LOGD(
            TAG,
            "[sensor fusion] sample=%lu fused_roll_x10=%ld "
            "altitude=%d m state=%s depth=%u",
            (unsigned long)sample.sample_id,
            (long)fused_roll_x10,
            sample.altitude_m,
            flight_state,
            (unsigned)current_depth
        );

        hb_cons++;
    }
}

static void coordinator_task(void *arg)
{
    (void)arg;

    const EventBits_t wait_mask =
        EV_BIT_DATA_PRODUCED | EV_BIT_DATA_PROCESSED;

    for (;;) {
        const EventBits_t got = xEventGroupWaitBits(
            evt_group,
            wait_mask,
            pdTRUE,         /* Clear both bits after a successful rendezvous. */
            pdTRUE,         /* Wait for ALL requested bits. */
            portMAX_DELAY
        );

        if ((got & wait_mask) != wait_mask) {
            continue;
        }

        const int64_t task_start_us = esp_timer_get_time();

        ESP_LOGD(
            TAG,
            "[telemetry coordinator] production and fusion stages complete"
        );

#if !LATENCY_TEST_MODE
        /* Disabled during latency testing so only button ISR wakeups are timed. */
        xTaskNotifyGive(responder_handle);
#endif

        hb_coord++;

        const uint64_t elapsed_us =
            (uint64_t)(esp_timer_get_time() - task_start_us);

        if (xSemaphoreTake(status_mutex, portMAX_DELAY) == pdTRUE) {
            if (elapsed_us > system_status.coordinator_wcet_us) {
                system_status.coordinator_wcet_us = elapsed_us;
            }
            xSemaphoreGive(status_mutex);
        }
    }
}

static void responder_task(void *arg)
{
    (void)arg;

    for (;;) {
        const uint32_t notification_count =
            ulTaskNotifyTake(pdTRUE, portMAX_DELAY);

        if (notification_count == 0U) {
            continue;
        }

#if LATENCY_TEST_MODE
        /* In test mode every notification comes from the debounced button ISR. */
        const int64_t wake_us = esp_timer_get_time();
        const uint64_t latency_us =
            (uint64_t)(wake_us - isr_entry_time_us);

        notif_samples++;
        notif_latency_sum_us += latency_us;

        if (notif_samples == 1U || latency_us < notif_latency_min_us) {
            notif_latency_min_us = latency_us;
        }
        if (latency_us > notif_latency_max_us) {
            notif_latency_max_us = latency_us;
        }

        if (notif_samples == 50U) {
            ESP_LOGI(
                TAG,
                "[NOTIF SUMMARY] samples=%lu min=%llu us avg=%llu us "
                "max=%llu us",
                (unsigned long)notif_samples,
                (unsigned long long)notif_latency_min_us,
                (unsigned long long)(notif_latency_sum_us / notif_samples),
                (unsigned long long)notif_latency_max_us
            );
        }
#endif

        const int64_t task_start_us = esp_timer_get_time();

        ESP_LOGD(
            TAG,
            "[flight responder] telemetry/status response triggered "
            "(count=%lu)",
            (unsigned long)notification_count
        );

        hb_resp++;

        const uint64_t elapsed_us =
            (uint64_t)(esp_timer_get_time() - task_start_us);

        if (xSemaphoreTake(status_mutex, portMAX_DELAY) == pdTRUE) {
            if (elapsed_us > system_status.responder_wcet_us) {
                system_status.responder_wcet_us = elapsed_us;
            }
            xSemaphoreGive(status_mutex);
        }
    }
}

/* -------------------------------------------------------------------------- */
/* Button ISR and optional latency-comparison task                            */
/* -------------------------------------------------------------------------- */

static void IRAM_ATTR button_isr(void *arg)
{
    (void)arg;

    const int64_t now_us = esp_timer_get_time();

    if ((now_us - last_edge_us) < DEBOUNCE_US) {
        return;
    }

    last_edge_us = now_us;
    presses_observed++;

    BaseType_t higher_priority_task_woken = pdFALSE;

#if LATENCY_TEST_MODE
    isr_entry_time_us = now_us;
    xSemaphoreGiveFromISR(
        latency_sem,
        &higher_priority_task_woken
    );
#endif

    vTaskNotifyGiveFromISR(
        responder_handle,
        &higher_priority_task_woken
    );

    portYIELD_FROM_ISR(higher_priority_task_woken);
}

#if LATENCY_TEST_MODE
static void sem_latency_task(void *arg)
{
    (void)arg;

    for (;;) {
        if (xSemaphoreTake(latency_sem, portMAX_DELAY) != pdTRUE) {
            continue;
        }

        const int64_t wake_us = esp_timer_get_time();
        const uint64_t latency_us =
            (uint64_t)(wake_us - isr_entry_time_us);

        sem_samples++;
        sem_latency_sum_us += latency_us;

        if (sem_samples == 1U || latency_us < sem_latency_min_us) {
            sem_latency_min_us = latency_us;
        }
        if (latency_us > sem_latency_max_us) {
            sem_latency_max_us = latency_us;
        }

        if (sem_samples == 50U) {
            ESP_LOGI(
                TAG,
                "[SEMAPHORE SUMMARY] samples=%lu min=%llu us avg=%llu us "
                "max=%llu us",
                (unsigned long)sem_samples,
                (unsigned long long)sem_latency_min_us,
                (unsigned long long)(sem_latency_sum_us / sem_samples),
                (unsigned long long)sem_latency_max_us
            );
        }
    }
}
#endif

/* -------------------------------------------------------------------------- */
/* Core-0 web monitor                                                         */
/* -------------------------------------------------------------------------- */

#if USE_WEBSERVER

static esp_err_t handle_state(httpd_req_t *req)
{
    const UBaseType_t depth = uxQueueMessagesWaiting(data_q);
    const EventBits_t bits = xEventGroupGetBits(evt_group);

    system_status_t status = {0};

    if (!get_status_snapshot(&status)) {
        httpd_resp_set_status(req, "503 Service Unavailable");
        return httpd_resp_send(
            req,
            "System status unavailable",
            HTTPD_RESP_USE_STRLEN
        );
    }

    char buf[1024];

    snprintf(
        buf,
        sizeof(buf),
        "{"
            "\"q_depth\":%u,"
            "\"q_capacity\":%u,"
            "\"evt\":%u,"
            "\"last_id\":%lu,"
            "\"roll_x10\":%ld,"
            "\"altitude_m\":%ld,"
            "\"dropped_samples\":%lu,"
            "\"queue_high_watermark\":%lu,"
            "\"producer_wcet_us\":%llu,"
            "\"consumer_wcet_us\":%llu,"
            "\"coordinator_wcet_us\":%llu,"
            "\"responder_wcet_us\":%llu,"
            "\"fault_active\":%s,"
            "\"degraded\":%s,"
            "\"button_presses\":%lu,"
            "\"hb_prod\":%lu,"
            "\"hb_cons\":%lu,"
            "\"hb_coord\":%lu,"
            "\"hb_resp\":%lu"
        "}",
        (unsigned)depth,
        (unsigned)DATA_Q_DEPTH,
        (unsigned)bits,
        (unsigned long)status.last_sample_id,
        (long)status.fused_roll_x10,
        (long)status.altitude_m,
        (unsigned long)status.dropped_samples,
        (unsigned long)status.queue_high_watermark,
        (unsigned long long)status.producer_wcet_us,
        (unsigned long long)status.consumer_wcet_us,
        (unsigned long long)status.coordinator_wcet_us,
        (unsigned long long)status.responder_wcet_us,
        status.fault_active ? "true" : "false",
        status.degraded ? "true" : "false",
        (unsigned long)presses_observed,
        (unsigned long)hb_prod,
        (unsigned long)hb_cons,
        (unsigned long)hb_coord,
        (unsigned long)hb_resp
    );

    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Cache-Control", "no-store");

    return httpd_resp_send(req, buf, HTTPD_RESP_USE_STRLEN);
}

static esp_err_t handle_fault_toggle(httpd_req_t *req)
{
    bool enabled = false;

    if (xSemaphoreTake(status_mutex, pdMS_TO_TICKS(20)) != pdTRUE) {
        httpd_resp_set_status(req, "503 Service Unavailable");
        return httpd_resp_send(
            req,
            "Unable to update fault state",
            HTTPD_RESP_USE_STRLEN
        );
    }

    system_status.fault_active = !system_status.fault_active;
    enabled = system_status.fault_active;

    if (enabled) {
        system_status.degraded = true;
    }

    xSemaphoreGive(status_mutex);

    ESP_LOGW(
        TAG,
        "[fault injection] sensor-fusion stall %s",
        enabled ? "ENABLED" : "DISABLED"
    );

    char response[64];
    snprintf(
        response,
        sizeof(response),
        "{\"fault_active\":%s}",
        enabled ? "true" : "false"
    );

    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Cache-Control", "no-store");

    return httpd_resp_send(req, response, HTTPD_RESP_USE_STRLEN);
}

static esp_err_t handle_root(httpd_req_t *req)
{
    static const char html[] =
        "<!DOCTYPE html>"
        "<html lang=\"en\">"
        "<head>"
        "<meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
        "<title>Avionics Telemetry and Fault Monitor</title>"
        "<style>"
        "*{box-sizing:border-box;}"
        "body{margin:0;font-family:Arial,sans-serif;background:#eef2f6;color:#17212b;}"
        "header{background:#102a43;color:white;padding:28px 20px;}"
        "header h1{margin:0 0 8px;}"
        "header p{margin:0;color:#c9d8e6;}"
        ".wrap{max-width:1050px;margin:24px auto;padding:0 18px 40px;}"
        ".statusbar{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:18px;}"
        ".badge{padding:10px 14px;border-radius:999px;font-weight:bold;}"
        ".good{background:#d8f3dc;color:#176b2c;}"
        ".bad{background:#ffd6d6;color:#9b1c1c;}"
        ".neutral{background:#dbeafe;color:#174ea6;}"
        ".grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:16px;}"
        ".card{background:white;border-radius:12px;padding:18px;box-shadow:0 3px 14px rgba(16,42,67,.12);}"
        ".card h2{margin:0 0 12px;font-size:1.05rem;color:#243b53;}"
        ".row{display:flex;justify-content:space-between;gap:16px;padding:8px 0;border-bottom:1px solid #e7edf3;}"
        ".row:last-child{border-bottom:none;}"
        ".value{font-weight:bold;font-variant-numeric:tabular-nums;text-align:right;}"
        "button{margin-top:16px;border:0;border-radius:8px;padding:12px 16px;font-weight:bold;cursor:pointer;background:#c62828;color:white;}"
        "button.remove{background:#176b2c;}"
        ".small{font-size:.88rem;color:#627d98;margin-top:10px;}"
        "</style>"
        "</head>"
        "<body>"
        "<header>"
        "<h1>Dual-Core Avionics Telemetry System</h1>"
        "<p>FreeRTOS IPC, timing evidence, fault injection, and graceful recovery</p>"
        "</header>"
        "<main class=\"wrap\">"
        "<div class=\"statusbar\">"
        "<span id=\"system_state\" class=\"badge neutral\">CONNECTING</span>"
        "<span id=\"fault_state\" class=\"badge neutral\">Fault: --</span>"
        "</div>"
        "<div class=\"grid\">"
        "<section class=\"card\">"
        "<h2>Live avionics data</h2>"
        "<div class=\"row\"><span>Last sample ID</span><span id=\"last_id\" class=\"value\">--</span></div>"
        "<div class=\"row\"><span>Fused roll</span><span id=\"roll\" class=\"value\">--</span></div>"
        "<div class=\"row\"><span>Altitude</span><span id=\"altitude\" class=\"value\">--</span></div>"
        "<div class=\"row\"><span>Event bits</span><span id=\"evt\" class=\"value\">--</span></div>"
        "</section>"
        "<section class=\"card\">"
        "<h2>Queue and degradation evidence</h2>"
        "<div class=\"row\"><span>Current queue depth</span><span id=\"q_depth\" class=\"value\">--</span></div>"
        "<div class=\"row\"><span>Queue high-water mark</span><span id=\"q_high\" class=\"value\">--</span></div>"
        "<div class=\"row\"><span>Dropped samples</span><span id=\"drops\" class=\"value\">--</span></div>"
        "<div class=\"row\"><span>Button presses</span><span id=\"presses\" class=\"value\">--</span></div>"
        "<button id=\"fault_button\" onclick=\"toggleFault()\">Inject Sensor Fusion Stall</button>"
        "<div class=\"small\">The injected 300 ms processing stall should fill the four-item queue, cause drops, and place the system in DEGRADED mode.</div>"
        "</section>"
        "<section class=\"card\">"
        "<h2>Measured maximum task times</h2>"
        "<div class=\"row\"><span>Attitude Sensor</span><span id=\"wcet_prod\" class=\"value\">--</span></div>"
        "<div class=\"row\"><span>Sensor Fusion</span><span id=\"wcet_cons\" class=\"value\">--</span></div>"
        "<div class=\"row\"><span>Coordinator</span><span id=\"wcet_coord\" class=\"value\">--</span></div>"
        "<div class=\"row\"><span>Responder</span><span id=\"wcet_resp\" class=\"value\">--</span></div>"
        "</section>"
        "<section class=\"card\">"
        "<h2>Task heartbeats</h2>"
        "<div class=\"row\"><span>Producer</span><span id=\"hb_prod\" class=\"value\">--</span></div>"
        "<div class=\"row\"><span>Consumer</span><span id=\"hb_cons\" class=\"value\">--</span></div>"
        "<div class=\"row\"><span>Coordinator</span><span id=\"hb_coord\" class=\"value\">--</span></div>"
        "<div class=\"row\"><span>Responder</span><span id=\"hb_resp\" class=\"value\">--</span></div>"
        "</section>"
        "</div>"
        "</main>"
        "<script>"
        "async function poll(){"
            "try{"
                "const response=await fetch('/state',{cache:'no-store'});"
                "if(!response.ok){throw new Error('state request failed');}"
                "const s=await response.json();"
                "const state=document.getElementById('system_state');"
                "state.textContent=s.degraded?'DEGRADED':'NOMINAL';"
                "state.className='badge '+(s.degraded?'bad':'good');"
                "const fault=document.getElementById('fault_state');"
                "fault.textContent='Fault: '+(s.fault_active?'ACTIVE':'OFF');"
                "fault.className='badge '+(s.fault_active?'bad':'good');"
                "document.getElementById('q_depth').textContent=s.q_depth+' / '+s.q_capacity;"
                "document.getElementById('q_high').textContent=s.queue_high_watermark+' / '+s.q_capacity;"
                "document.getElementById('drops').textContent=s.dropped_samples;"
                "document.getElementById('presses').textContent=s.button_presses;"
                "document.getElementById('evt').textContent='0x'+Number(s.evt).toString(16).padStart(2,'0');"
                "document.getElementById('last_id').textContent=s.last_id;"
                "document.getElementById('roll').textContent=(s.roll_x10/10).toFixed(1)+' degrees';"
                "document.getElementById('altitude').textContent=s.altitude_m+' m';"
                "document.getElementById('wcet_prod').textContent=s.producer_wcet_us+' us';"
                "document.getElementById('wcet_cons').textContent=s.consumer_wcet_us+' us';"
                "document.getElementById('wcet_coord').textContent=s.coordinator_wcet_us+' us';"
                "document.getElementById('wcet_resp').textContent=s.responder_wcet_us+' us';"
                "document.getElementById('hb_prod').textContent=s.hb_prod;"
                "document.getElementById('hb_cons').textContent=s.hb_cons;"
                "document.getElementById('hb_coord').textContent=s.hb_coord;"
                "document.getElementById('hb_resp').textContent=s.hb_resp;"
                "const button=document.getElementById('fault_button');"
                "button.textContent=s.fault_active?'Remove Sensor Fusion Stall':'Inject Sensor Fusion Stall';"
                "button.className=s.fault_active?'remove':'';"
            "}catch(error){"
                "const state=document.getElementById('system_state');"
                "state.textContent='MONITOR OFFLINE';"
                "state.className='badge bad';"
            "}"
        "}"
        "async function toggleFault(){"
            "const button=document.getElementById('fault_button');"
            "button.disabled=true;"
            "try{"
                "await fetch('/fault/toggle',{method:'POST',cache:'no-store'});"
                "await poll();"
            "}finally{"
                "button.disabled=false;"
            "}"
        "}"
        "setInterval(poll,1000);"
        "poll();"
        "</script>"
        "</body>"
        "</html>";

    httpd_resp_set_type(req, "text/html");
    httpd_resp_set_hdr(req, "Cache-Control", "no-store");
    return httpd_resp_send(req, html, HTTPD_RESP_USE_STRLEN);
}

static httpd_handle_t start_webserver(void)
{
    if (server_handle != NULL) {
        return server_handle;
    }

    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.server_port = HTTP_PORT;
    config.core_id = PRO_CPU_NUM;
    config.task_priority = 5;
    config.stack_size = 8192;

    if (httpd_start(&server_handle, &config) != ESP_OK) {
        ESP_LOGE(TAG, "HTTP server failed to start");
        server_handle = NULL;
        return NULL;
    }

    const httpd_uri_t root = {
        .uri = "/",
        .method = HTTP_GET,
        .handler = handle_root,
        .user_ctx = NULL
    };

    const httpd_uri_t state = {
        .uri = "/state",
        .method = HTTP_GET,
        .handler = handle_state,
        .user_ctx = NULL
    };

    const httpd_uri_t fault_toggle = {
        .uri = "/fault/toggle",
        .method = HTTP_POST,
        .handler = handle_fault_toggle,
        .user_ctx = NULL
    };

    ESP_ERROR_CHECK(httpd_register_uri_handler(server_handle, &root));
    ESP_ERROR_CHECK(httpd_register_uri_handler(server_handle, &state));
    ESP_ERROR_CHECK(httpd_register_uri_handler(server_handle, &fault_toggle));

    ESP_LOGI(TAG, "HTTP server started on port %d", HTTP_PORT);
    return server_handle;
}

static void wifi_event_handler(
    void *arg,
    esp_event_base_t event_base,
    int32_t event_id,
    void *event_data
)
{
    (void)arg;

    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
        return;
    }

    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        ESP_LOGW(TAG, "Wi-Fi disconnected; reconnecting...");
        esp_wifi_connect();
        return;
    }

    if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        const ip_event_got_ip_t *event =
            (const ip_event_got_ip_t *)event_data;

        ESP_LOGI(
            TAG,
            "Got IP: " IPSTR,
            IP2STR(&event->ip_info.ip)
        );

        start_webserver();
    }
}

static void wifi_init_sta(void)
{
    esp_err_t result = nvs_flash_init();

    if (result == ESP_ERR_NVS_NO_FREE_PAGES ||
        result == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        result = nvs_flash_init();
    }

    ESP_ERROR_CHECK(result);
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    esp_netif_create_default_wifi_sta();

    const wifi_init_config_t init_config = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&init_config));

    ESP_ERROR_CHECK(
        esp_event_handler_register(
            WIFI_EVENT,
            ESP_EVENT_ANY_ID,
            &wifi_event_handler,
            NULL
        )
    );

    ESP_ERROR_CHECK(
        esp_event_handler_register(
            IP_EVENT,
            IP_EVENT_STA_GOT_IP,
            &wifi_event_handler,
            NULL
        )
    );

    wifi_config_t wifi_config = {
        .sta = {
            .ssid = WIFI_SSID,
            .password = WIFI_PASS,
            .threshold.authmode = WIFI_AUTH_OPEN,
        },
    };

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());
}

static void webmonitor_task(void *arg)
{
    (void)arg;

    ESP_LOGI(TAG, "[web monitor] starting Wi-Fi and HTTP dashboard");
    wifi_init_sta();

    for (;;) {
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}

#else

/* -------------------------------------------------------------------------- */
/* Core-0 serial monitor                                                      */
/* -------------------------------------------------------------------------- */

static void serial_monitor_task(void *arg)
{
    (void)arg;

    for (;;) {
        const UBaseType_t depth = uxQueueMessagesWaiting(data_q);
        const EventBits_t bits = xEventGroupGetBits(evt_group);
        system_status_t status = {0};

        if (get_status_snapshot(&status)) {
            ESP_LOGI(
                TAG,
                "[monitor] state=%s fault=%s q=%u/%u high=%lu drops=%lu "
                "last=%lu roll_x10=%ld altitude=%ld "
                "wcet_us[p=%llu c=%llu coord=%llu resp=%llu] "
                "hb[p=%lu c=%lu coord=%lu resp=%lu] presses=%lu evt=0x%02x",
                status.degraded ? "DEGRADED" : "NOMINAL",
                status.fault_active ? "ON" : "OFF",
                (unsigned)depth,
                (unsigned)DATA_Q_DEPTH,
                (unsigned long)status.queue_high_watermark,
                (unsigned long)status.dropped_samples,
                (unsigned long)status.last_sample_id,
                (long)status.fused_roll_x10,
                (long)status.altitude_m,
                (unsigned long long)status.producer_wcet_us,
                (unsigned long long)status.consumer_wcet_us,
                (unsigned long long)status.coordinator_wcet_us,
                (unsigned long long)status.responder_wcet_us,
                (unsigned long)hb_prod,
                (unsigned long)hb_cons,
                (unsigned long)hb_coord,
                (unsigned long)hb_resp,
                (unsigned long)presses_observed,
                (unsigned)bits
            );
        } else {
            ESP_LOGW(TAG, "[monitor] system-status snapshot unavailable");
        }

#if LATENCY_TEST_MODE
        ESP_LOGI(
            TAG,
            "[latency test] accepted_presses=%lu notif_samples=%lu "
            "sem_samples=%lu",
            (unsigned long)presses_observed,
            (unsigned long)notif_samples,
            (unsigned long)sem_samples
        );
#endif

        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}

#endif /* USE_WEBSERVER */

/* -------------------------------------------------------------------------- */
/* Application entry point                                                    */
/* -------------------------------------------------------------------------- */

void app_main(void)
{
    esp_log_level_set(TAG, ESP_LOG_INFO);

    ESP_LOGI(
        TAG,
        "==== Dual-Core Avionics Telemetry and Fault Monitor starting ===="
    );

#if USE_WEBSERVER
    ESP_LOGI(TAG, "Observability: HTTP dashboard on Core 0");
#else
    ESP_LOGI(TAG, "Observability: serial monitor on Core 0");
#endif

#if LATENCY_TEST_MODE
    ESP_LOGW(TAG, "LATENCY_TEST_MODE enabled — coordinator notification disabled");
#else
    ESP_LOGI(TAG, "Integrated pipeline mode enabled");
#endif

    data_q = xQueueCreate(DATA_Q_DEPTH, sizeof(attitude_sample_t));
    if (data_q == NULL) {
        ESP_LOGE(TAG, "Failed to create attitude-data queue");
        return;
    }

    evt_group = xEventGroupCreate();
    if (evt_group == NULL) {
        ESP_LOGE(TAG, "Failed to create avionics event group");
        vQueueDelete(data_q);
        return;
    }

    status_mutex = xSemaphoreCreateMutex();
    if (status_mutex == NULL) {
        ESP_LOGE(TAG, "Failed to create system-status mutex");
        vEventGroupDelete(evt_group);
        vQueueDelete(data_q);
        return;
    }

#if LATENCY_TEST_MODE
    latency_sem = xSemaphoreCreateBinary();
    if (latency_sem == NULL) {
        ESP_LOGE(TAG, "Failed to create latency semaphore");
        vSemaphoreDelete(status_mutex);
        vEventGroupDelete(evt_group);
        vQueueDelete(data_q);
        return;
    }
#endif

    BaseType_t task_result;

    /*
     * Create the notification receiver first so responder_handle is valid
     * before the coordinator or GPIO ISR can notify it.
     */
    task_result = xTaskCreatePinnedToCore(
        responder_task,
        "flight_resp",
        4096,
        NULL,
        12,
        &responder_handle,
        APP_CPU_NUM
    );

    if (task_result != pdPASS || responder_handle == NULL) {
        ESP_LOGE(TAG, "Failed to create Flight Responder task");
        return;
    }

    task_result = xTaskCreatePinnedToCore(
        coordinator_task,
        "telemetry",
        4096,
        NULL,
        9,
        NULL,
        APP_CPU_NUM
    );

    if (task_result != pdPASS) {
        ESP_LOGE(TAG, "Failed to create Telemetry Coordinator task");
        return;
    }

    task_result = xTaskCreatePinnedToCore(
        consumer_task,
        "fusion",
        4096,
        NULL,
        8,
        NULL,
        APP_CPU_NUM
    );

    if (task_result != pdPASS) {
        ESP_LOGE(TAG, "Failed to create Sensor Fusion task");
        return;
    }

    task_result = xTaskCreatePinnedToCore(
        producer_task,
        "att_sensor",
        4096,
        NULL,
        8,
        NULL,
        APP_CPU_NUM
    );

    if (task_result != pdPASS) {
        ESP_LOGE(TAG, "Failed to create Attitude Sensor task");
        return;
    }

#if LATENCY_TEST_MODE
    task_result = xTaskCreatePinnedToCore(
        sem_latency_task,
        "sem_latency",
        4096,
        NULL,
        12,
        NULL,
        APP_CPU_NUM
    );

    if (task_result != pdPASS) {
        ESP_LOGE(TAG, "Failed to create semaphore-latency task");
        return;
    }
#endif

#if USE_WEBSERVER
    task_result = xTaskCreatePinnedToCore(
        webmonitor_task,
        "webmon",
        4096,
        NULL,
        4,
        NULL,
        PRO_CPU_NUM
    );
#else
    task_result = xTaskCreatePinnedToCore(
        serial_monitor_task,
        "monitor",
        4096,
        NULL,
        4,
        NULL,
        PRO_CPU_NUM
    );
#endif

    if (task_result != pdPASS) {
        ESP_LOGE(TAG, "Failed to create Core-0 observability task");
        return;
    }

    const gpio_config_t button_config = {
        .pin_bit_mask = 1ULL << BUTTON_GPIO,
        .mode = GPIO_MODE_INPUT,
        .pull_up_en = GPIO_PULLUP_ENABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_NEGEDGE,
    };

    ESP_ERROR_CHECK(gpio_config(&button_config));

    esp_err_t isr_result = gpio_install_isr_service(0);
    if (isr_result != ESP_OK && isr_result != ESP_ERR_INVALID_STATE) {
        ESP_ERROR_CHECK(isr_result);
    }

    ESP_ERROR_CHECK(
        gpio_isr_handler_add(BUTTON_GPIO, button_isr, NULL)
    );

    ESP_LOGI(
        TAG,
        "Initialization complete — queue item size=%u bytes",
        (unsigned)sizeof(attitude_sample_t)
    );
}
