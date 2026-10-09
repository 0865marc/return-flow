import http from 'k6/http';
import { check } from 'k6';
import execution from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';

function positiveInteger(name, fallback) {
  const value = Number(__ENV[name] || fallback);
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

const baseUrl = (__ENV.BASE_URL || 'http://api:8000').replace(/\/$/, '');
const scenario = __ENV.SCENARIO || 'flow';
const seedSize = positiveInteger('SEED_SIZE', 1000);
const preAllocatedVUs = positiveInteger('PREALLOCATED_VUS', 20);
const maxVUs = positiveInteger('MAX_VUS', 200);
const timeoutSetting = __ENV.REQUEST_TIMEOUT || '5s';
const requestTimeoutSeconds = Number(timeoutSetting.slice(0, -1));
if (!timeoutSetting.endsWith('s') || !Number.isFinite(requestTimeoutSeconds)
    || requestTimeoutSeconds <= 0) {
  throw new Error('REQUEST_TIMEOUT must be a positive number of seconds, e.g. 5s');
}
if (!['read', 'create', 'flow'].includes(scenario)) {
  throw new Error('SCENARIO must be read, create or flow');
}
if (maxVUs < preAllocatedVUs) throw new Error('MAX_VUS must be >= PREALLOCATED_VUS');

const attempted = new Counter('workflows_attempted');
const completed = new Counter('workflows_completed');
const errors = new Counter('technical_errors');
const unknownWrites = new Counter('unknown_writes');
const deliveries = new Counter('deliveries_created');
const delivered = new Counter('deliveries_delivered');
const returns = new Counter('returns_created');
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
  'GET /deliveries/{id}': new Trend('get_delivery_duration', true),
  'POST /deliveries': new Trend('create_delivery_duration', true),
  'POST /deliveries/{id}/deliver': new Trend('deliver_delivery_duration', true),
  'POST /returns': new Trend('create_return_duration', true),
  'GET /returns/{id}': new Trend('get_return_duration', true),
};
let metricsInitialized = false;

export const options = {
  throw: false,
  scenarios: {
    workload: {
      executor: 'constant-arrival-rate',
      rate: positiveInteger('RATE', 10),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs,
      maxVUs,
      // Allow every request of an in-flight workflow to finish, plus a margin.
      gracefulStop: `${Math.ceil(4 * requestTimeoutSeconds + 10)}s`,
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
    workflows_completed: ['count>0'],
  },
};

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function isId(value) {
  return typeof value === 'string' && uuid.test(value);
}

function request(method, path, name, status, validBody, body = null) {
  httpRequests.total.add(1, { name });
  const response = http.request(method, `${baseUrl}${path}`, body, {
    headers: { 'Content-Type': 'application/json' },
    tags: { name },
    timeout: requestTimeoutSeconds * 1000,
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
      && payload !== null && typeof payload === 'object' && validBody(payload),
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
    return null;
  }
  httpRequests.succeeded.add(1, { name });
  return payload;
}

export default function () {
  if (!metricsInitialized) {
    [attempted, completed, errors, unknownWrites, deliveries, delivered, returns,
      ...Object.values(httpRequests)]
      .forEach((metric) => metric.add(0));
    metricsInitialized = true;
  }
  attempted.add(1);
  const started = Date.now();
  const receipt = {};
  try {
    if (scenario === 'read') {
      const index = execution.scenario.iterationInTest % seedSize + 1;
      const id = `00000000-0000-4000-8000-${String(index).padStart(12, '0')}`;
      if (!request('GET', `/deliveries/${id}`, 'GET /deliveries/{id}', 200,
        (data) => data.id === id && data.status === 'delivered')) return;
    } else {
      const delivery = request('POST', '/deliveries', 'POST /deliveries', 201,
        (data) => isId(data.id) && data.status === 'pending');
      if (!delivery) return;
      receipt.delivery_id = delivery.id;
      deliveries.add(1);

      if (scenario === 'flow') {
        if (!request('POST', `/deliveries/${delivery.id}/deliver`,
          'POST /deliveries/{id}/deliver', 200,
          (data) => data.id === delivery.id && data.status === 'delivered')) return;
        receipt.delivered_id = delivery.id;
        delivered.add(1);

        const returned = request('POST', '/returns', 'POST /returns', 201,
          (data) => isId(data.id) && data.delivery_id === delivery.id
            && data.status === 'requested', JSON.stringify({ delivery_id: delivery.id }));
        if (!returned) return;
        receipt.return_id = returned.id;
        receipt.return_delivery_id = delivery.id;
        returns.add(1);

        if (!request('GET', `/returns/${returned.id}`, 'GET /returns/{id}', 200,
          (data) => data.id === returned.id && data.delivery_id === delivery.id
            && data.status === 'requested')) return;
      }
    }
    completed.add(1);
    // Only completed, correct workflows contribute to completion latency.
    workflowDuration.add(Date.now() - started);
  } finally {
    // Preserve confirmed writes even if a later step fails; no automatic retries.
    if (Object.keys(receipt).length) console.log(`RECEIPT ${JSON.stringify(receipt)}`);
  }
}

export function handleSummary(data) {
  const count = (name) => data.metrics[name]?.values.count || 0;
  const latency = data.metrics.workflow_duration?.values || {};
  return {
    [__ENV.SUMMARY_PATH || '/results/summary.json']: JSON.stringify(data, null, 2),
    stdout: `${scenario}: ${count('workflows_completed')}/${count('workflows_attempted')}`
      + ` completed; errors=${count('technical_errors')}`
      + ` unknown_writes=${count('unknown_writes')}`
      + ` dropped=${count('dropped_iterations')}`
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
