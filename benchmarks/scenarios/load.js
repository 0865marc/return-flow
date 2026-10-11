import http from 'k6/http';
import { check, sleep } from 'k6';
import crypto from 'k6/crypto';
import execution from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';

function positiveInteger(name, fallback) {
  const value = Number(__ENV[name] || fallback);
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function positiveSeconds(name, fallback) {
  const value = Number(__ENV[name] || fallback);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive number of seconds`);
  }
  return value;
}

const baseUrl = (__ENV.BASE_URL || 'http://api:8000').replace(/\/$/, '');
const scenario = __ENV.SCENARIO || 'flow';
const eventDriven = __ENV.VARIANT === 'event_driven';
const commandTimeoutSeconds = positiveSeconds('COMMAND_TIMEOUT', 10);
const pollIntervalSeconds = positiveSeconds('POLL_INTERVAL', 0.1);
const pollMaxIntervalSeconds = positiveSeconds('POLL_MAX_INTERVAL', 1);
if (pollMaxIntervalSeconds < pollIntervalSeconds) {
  throw new Error('POLL_MAX_INTERVAL must be >= POLL_INTERVAL');
}
const durationSetting = __ENV.DURATION || '30s';
const durationSeconds = Number(durationSetting.slice(0, -1));
if (!durationSetting.endsWith('s') || !Number.isFinite(durationSeconds) || durationSeconds <= 0) {
  throw new Error('DURATION must be a positive number of seconds');
}
const preAllocatedVUs = positiveInteger('PREALLOCATED_VUS', 20);
const maxVUs = positiveInteger('MAX_VUS', 200);
const timeoutSetting = __ENV.REQUEST_TIMEOUT || '5s';
const requestTimeoutSeconds = Number(timeoutSetting.slice(0, -1));
if (!timeoutSetting.endsWith('s') || !Number.isFinite(requestTimeoutSeconds)
    || requestTimeoutSeconds <= 0) {
  throw new Error('REQUEST_TIMEOUT must be a positive number of seconds, e.g. 5s');
}
if (scenario !== 'flow') throw new Error('Only complete delivery-to-return flows are measured');
if (maxVUs < preAllocatedVUs) throw new Error('MAX_VUS must be >= PREALLOCATED_VUS');

const attempted = new Counter('workflows_attempted');
const completed = new Counter('workflows_completed');
const submitted = new Counter('workflows_submitted');
const loadStarted = new Trend('load_started_at');
const errors = new Counter('technical_errors');
const unknownWrites = new Counter('unknown_writes');
const deliveries = new Counter('deliveries_created');
const delivered = new Counter('deliveries_delivered');
const returns = new Counter('returns_created');
const commands = {
  submitted: new Counter('commands_submitted'),
  completed: new Counter('commands_completed'),
  failed: new Counter('commands_failed'),
  timedOut: new Counter('commands_timed_out'),
};
const httpRequests = {
  total: new Counter('http_requests_total'),
  succeeded: new Counter('http_requests_succeeded'),
  failed: new Counter('http_requests_failed'),
  responses4xx: new Counter('http_responses_4xx'),
  responses5xx: new Counter('http_responses_5xx'),
  transportErrors: new Counter('http_transport_errors'),
  timeouts: new Counter('http_timeouts'),
  unexpectedResponses: new Counter('http_unexpected_responses'),
};
const workflowDuration = new Trend('workflow_duration', true);
const requestDurations = {
  'POST /deliveries': new Trend('create_delivery_duration', true),
  'POST /deliveries/{id}/deliver': new Trend('deliver_delivery_duration', true),
  'POST /returns': new Trend('create_return_duration', true),
  'POST /deliveries-async': new Trend('create_delivery_async_duration', true),
  'POST /deliveries/{id}/deliver-async': new Trend('deliver_delivery_async_duration', true),
  'POST /returns-async': new Trend('create_return_async_duration', true),
  'GET /commands/{id}': new Trend('get_command_duration', true),
};
let metricsInitialized = false;

export const options = {
  throw: false,
  scenarios: {
    workload: {
      executor: 'constant-arrival-rate',
      rate: positiveInteger('RATE', 10),
      timeUnit: '1s',
      duration: durationSetting,
      preAllocatedVUs,
      maxVUs,
      // Only the first two commands must be observed to advance the flow.
      // The final outcome is audited in PostgreSQL after the load has stopped.
      gracefulStop: `${Math.ceil(3 * requestTimeoutSeconds + 10
        + (eventDriven ? 2 * commandTimeoutSeconds : 0))}s`,
    },
  },
  // Keep UUIDs out of metric labels; request names contain route templates.
  systemTags: ['status', 'method', 'name', 'scenario', 'expected_response', 'error_code'],
  summaryTrendStats: ['min', 'avg', 'med', 'p(50)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    checks: ['rate==1'],
    dropped_iterations: ['count==0'],
    technical_errors: ['count==0'],
    unknown_writes: ['count==0'],
    workflows_submitted: ['count>0'],
  },
};

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function isId(value) {
  return typeof value === 'string' && uuid.test(value);
}

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function newCommandId() {
  const bytes = new Uint8Array(crypto.randomBytes(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}`
    + `-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function request(method, path, name, status, validBody, body = null, settings = {}) {
  httpRequests.total.add(1, { name });
  const response = http.request(method, `${baseUrl}${path}`, body, {
    headers: { 'Content-Type': 'application/json', ...settings.headers },
    tags: { name },
    timeout: settings.timeoutMilliseconds ?? requestTimeoutSeconds * 1000,
    redirects: 0,
  });
  requestDurations[name].add(response.timings.duration);
  let payload = null;
  try {
    payload = response.json();
  } catch (_) {
    // Invalid/empty bodies are reported by the same check as schema violations.
  }
  const valid = check(response, {
    [`${name}: status and payload`]: () => response.status === status
      && isRecord(payload) && validBody(payload),
  });
  if (!valid) {
    errors.add(1, { name });
    httpRequests.failed.add(1, { name });
    // Each failed request belongs to exactly one category. Timeouts are a
    // subset of transport errors, so they must not be added to the total again.
    if (response.status === 0) {
      httpRequests.transportErrors.add(1, { name });
      // k6 documents 1050 as request timeout and 1211 as connection dial timeout.
      if (response.error_code === 1050 || response.error_code === 1211) {
        httpRequests.timeouts.add(1, { name });
      }
    } else if (response.status >= 400 && response.status < 500) {
      httpRequests.responses4xx.add(1, { name });
    } else if (response.status >= 500 && response.status < 600) {
      httpRequests.responses5xx.add(1, { name });
    } else {
      // Includes unexpected statuses and successful statuses with invalid bodies.
      httpRequests.unexpectedResponses.add(1, { name });
    }
    if (method !== 'GET' && (response.status === 0 || response.status >= 500
        || (response.status >= 200 && response.status < 300))) {
      // A timeout/5xx or unreadable success does not prove the write was rejected.
      unknownWrites.add(1, { name });
    }
    if (settings.onFailure) settings.onFailure(response);
    return null;
  }
  httpRequests.succeeded.add(1, { name });
  return payload;
}

function executeDirect(path, name, status, validResult, receipt, body = null) {
  const id = newCommandId();
  receipt.command_ids.push(id);
  commands.submitted.add(1);
  const result = request('POST', path, name, status, validResult, body,
    { headers: { 'Idempotency-Key': id } });
  if (result) {
    receipt.accepted_command_ids.push(id);
    commands.completed.add(1);
  }
  return result;
}

function submitCommand(path, name, type, validResult, receipt, body = null, observe = true) {
  const id = newCommandId();
  // Keep every attempted command for the database audit, including a POST
  // whose reply is lost. No POST is retried automatically by the benchmark.
  if (!receipt.command_ids) receipt.command_ids = [];
  receipt.command_ids.push(id);
  commands.submitted.add(1);
  const accepted = request('POST', path, name, 202,
    (data) => data.command_id === id && data.status === 'accepted', body,
    { headers: { 'Idempotency-Key': id } });
  if (!accepted) return null;
  receipt.accepted_command_ids.push(id);
  if (!observe) return accepted;

  let delay = pollIntervalSeconds;
  const deadline = Date.now() + commandTimeoutSeconds * 1000;
  while (Date.now() < deadline) {
    const command = request('GET', `/commands/${id}`, 'GET /commands/{id}', 200,
      (data) => data.command_id === id && data.type === type && (
        (data.status === 'pending' && data.result === null && data.error === null)
        || (data.status === 'completed' && isRecord(data.result)
          && validResult(data.result) && data.error === null)
        || (data.status === 'failed' && data.result === null
          && typeof data.error === 'string' && data.error.length > 0)
      ), null, {
        timeoutMilliseconds: Math.max(1,
          Math.min(requestTimeoutSeconds * 1000, deadline - Date.now())),
        onFailure: (response) => {
          if (response.status === 0
              && (response.error_code === 1050 || response.error_code === 1211)) {
            commands.timedOut.add(1);
          }
        },
      });
    if (!command) {
      // A failed status query says nothing about whether the worker committed.
      unknownWrites.add(1, { name });
      return null;
    }
    if (command.status === 'failed') {
      commands.failed.add(1);
      errors.add(1, { name });
      return null;
    }
    if (command.status === 'completed') {
      commands.completed.add(1);
      return command.result;
    }
    const remaining = (deadline - Date.now()) / 1000;
    if (remaining > 0) sleep(Math.min(delay, remaining));
    delay = Math.min(delay * 2, pollMaxIntervalSeconds);
  }
  commands.timedOut.add(1);
  errors.add(1, { name });
  unknownWrites.add(1, { name });
  return null;
}

export default function () {
  if (!metricsInitialized) {
    [attempted, completed, submitted, errors, unknownWrites, deliveries, delivered, returns,
      ...Object.values(httpRequests), ...Object.values(commands)]
      .forEach((metric) => metric.add(0));
    loadStarted.add(execution.scenario.startTime);
    metricsInitialized = true;
  }
  attempted.add(1);
  const receipt = { flow_id: newCommandId(), started_at_ms: Date.now(),
    command_ids: [], accepted_command_ids: [] };
  try {
    const validDelivery = (data) => isId(data.id) && data.status === 'pending';
    const delivery = eventDriven
      ? submitCommand('/deliveries-async', 'POST /deliveries-async', 'create_delivery',
        validDelivery, receipt)
      : executeDirect('/deliveries', 'POST /deliveries', 201, validDelivery, receipt);
    if (!delivery) return;
    receipt.delivery_id = delivery.id;
    deliveries.add(1);

    const validDelivered = (data) => data.id === delivery.id && data.status === 'delivered';
    const deliveryResult = eventDriven
      ? submitCommand(`/deliveries/${delivery.id}/deliver-async`,
        'POST /deliveries/{id}/deliver-async', 'ensure_delivery_delivered',
        validDelivered, receipt)
      : executeDirect(`/deliveries/${delivery.id}/deliver`,
        'POST /deliveries/{id}/deliver', 200, validDelivered, receipt);
    if (!deliveryResult) return;
    receipt.delivered_id = delivery.id;
    delivered.add(1);

    const validReturn = (data) => isId(data.id) && data.delivery_id === delivery.id
      && data.status === 'requested';
    const returnBody = JSON.stringify({ delivery_id: delivery.id });
    receipt.return_attempted = true;
    const returned = eventDriven
      ? submitCommand('/returns-async', 'POST /returns-async', 'request_return',
        validReturn, receipt, returnBody, false)
      : executeDirect('/returns', 'POST /returns', 201, validReturn, receipt, returnBody);
    if (!returned) return;
    submitted.add(1);
    if (!eventDriven) {
      receipt.return_id = returned.id;
      receipt.return_delivery_id = delivery.id;
      returns.add(1);
      completed.add(1);
      workflowDuration.add(Date.now() - receipt.started_at_ms);
    }
    // A 202 for the last command is submission, never proof of completion.
  } finally {
    receipt.finished_at_ms = Date.now();
    // One receipt for EVERY started flow, including failures before any write.
    console.log(`RECEIPT ${JSON.stringify(receipt)}`);
  }
}

export function handleSummary(data) {
  const count = (name) => data.metrics[name]?.values.count || 0;
  const latency = data.metrics.workflow_duration?.values || {};
  data.benchmark = {
    version: 5, scenario, variant: __ENV.VARIANT,
    started_at_ms: data.metrics.load_started_at?.values.min ?? null,
    offered_duration_ms: durationSeconds * 1000,
  };
  return {
    [__ENV.SUMMARY_PATH || '/results/summary.json']: JSON.stringify(data, null, 2),
    stdout: `${scenario}: ${count('workflows_submitted')}/${count('workflows_attempted')}`
      + ` submitted; E2E completion is audited after the run; errors=${count('technical_errors')}`
      + ` unknown_writes=${count('unknown_writes')}`
      + ` dropped=${count('dropped_iterations')}`
      + (eventDriven ? `; commands submitted=${count('commands_submitted')}`
        + ` completed=${count('commands_completed')} failed=${count('commands_failed')}`
        + ` timed_out=${count('commands_timed_out')}` : '')
      + `; HTTP total=${count('http_requests_total')}`
      + ` succeeded=${count('http_requests_succeeded')}`
      + ` failed=${count('http_requests_failed')}`
      + ` (4xx=${count('http_responses_4xx')}, 5xx=${count('http_responses_5xx')}`
      + `, transport=${count('http_transport_errors')}`
      + ` [timeouts=${count('http_timeouts')}]`
      + `, unexpected=${count('http_unexpected_responses')})`
      + `; workflow ms p50=${latency['p(50)'] ?? 'n/a'}`
      + ` p95=${latency['p(95)'] ?? 'n/a'} p99=${latency['p(99)'] ?? 'n/a'}\n`,
  };
}
