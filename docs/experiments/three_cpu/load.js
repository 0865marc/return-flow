// One local k6 instance; externally enforce a 4-CPU limit. No --out sink.
// Load performs POST requests and local counters only. Output is written afterwards.
import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import exec from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';

const config = JSON.parse(open(__ENV.CONFIG_FILE || '/results/config.json'));
const rate = Number(config.rate === undefined ? 2000 : config.rate);
const durationSeconds = Number(config.duration === undefined ? 15 : config.duration);
const timeoutSeconds = Number(config.timeout === undefined ? 10 : config.timeout);
const expected = Math.ceil(rate * durationSeconds);
const baseUrl = (__ENV.BASE_URL || 'http://api:8000').replace(/\/$/, '');
if (!Number.isInteger(rate) || rate < 1 || !Number.isFinite(durationSeconds)
    || durationSeconds <= 0 || !Number.isFinite(timeoutSeconds) || timeoutSeconds <= 0) {
  throw new Error('Invalid rate, duration or timeout in config.json');
}
if (!['postgres_pool', 'event_driven'].includes(config.mode) || config.operation !== 'return') {
  throw new Error('This benchmark supports postgres_pool or event_driven / return');
}
const asynchronous = config.mode === 'event_driven';
const requestPath = asynchronous ? '/returns-async' : '/returns';
const expectedStatus = asynchronous ? 202 : 201;
const expectedBodyStatus = asynchronous ? 'accepted' : 'requested';
const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const plan = new SharedArray('return-flow-three-cpu-plan', function () {
  const rows = JSON.parse(open(__ENV.PLAN_FILE || '/results/plan.json'));
  const seen = new Set();
  if (rows.length < expected) throw new Error('Plan does not cover the scheduled load');
  return rows.map(function (row) {
    if (row.operation !== 'return' || !row.command_id || !row.delivery_id
        || seen.has(row.command_id)) {
      throw new Error('Plan has an invalid operation, missing identity or duplicate command_id');
    }
    seen.add(row.command_id);
    return {
      command_id: row.command_id,
      delivery_id: row.delivery_id,
      body: JSON.stringify({ delivery_id: row.delivery_id }),
    };
  });
});

const started = new Counter('diag_iterations_started');
const returned = new Counter('diag_iterations_returned');
const requestsStarted = new Counter('diag_requests_started');
const responsesReceived = new Counter('diag_responses_received');
const accepted = new Counter('diag_accepted');
const failed = new Counter('diag_failed_iterations');
const transportFailures = new Counter('diag_transport_failures');
const unexpectedStatuses = new Counter('diag_unexpected_statuses');
const invalidBodies = new Counter('diag_invalid_bodies');
const planErrors = new Counter('diag_plan_errors');
// Wall clock includes connection setup/blocking, at millisecond precision.
const requestWall = new Trend('diag_request_wall_ms', true);
// Exactly one sample per local execution; no per-request timestamps or ID tags.
const scenarioStartEpoch = new Trend('diag_scenario_start_epoch_ms');
const firstRequestEpoch = new Trend('diag_first_request_epoch_ms');
// Interpretable as schedule delay only when no arrivals were dropped.
const startOffsetError = new Trend('diag_start_offset_error_ms', true);

http.setResponseCallback(http.expectedStatuses(expectedStatus));

export const options = {
  scenarios: {
    admission: {
      executor: 'constant-arrival-rate',
      rate: rate,
      timeUnit: '1s',
      duration: durationSeconds + 's',
      preAllocatedVUs: 512,
      maxVUs: 512,
      gracefulStop: (timeoutSeconds + 2) + 's',
    },
  },
  discardResponseBodies: false,
  maxRedirects: 0,
  noConnectionReuse: false,
  noVUConnectionReuse: false,
  summaryTrendStats: ['count', 'avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    dropped_iterations: ['count==0'],
    diag_requests_started: ['count==' + expected],
    diag_failed_iterations: ['count==0'],
  },
};

export default function () {
  started.add(1);
  // Unique sequence of STARTED iterations; drops do not consume plan IDs.
  const index = exec.scenario.iterationInTest;
  if (index >= expected || index >= plan.length) {
    planErrors.add(1);
    failed.add(1);
    returned.add(1);
    return;
  }
  const item = plan[index];
  let response = null;
  const before = Date.now();
  startOffsetError.add(before - exec.scenario.startTime - index * 1000 / rate);
  if (index === 0) {
    scenarioStartEpoch.add(exec.scenario.startTime);
    firstRequestEpoch.add(before);
  }
  requestsStarted.add(1);
  try {
    response = http.post(baseUrl + requestPath, item.body, {
      timeout: timeoutSeconds + 's',
      redirects: 0,
      responseType: 'text',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': item.command_id,
      },
      tags: { name: 'POST ' + requestPath },
    });
  } catch (_) {
    // Local exception counted below; no logging inside the load window.
  } finally {
    requestWall.add(Date.now() - before);
  }

  const transportFailed = response === null || response.status === 0;
  if (!transportFailed) responsesReceived.add(1);
  let body = null;
  if (!transportFailed) {
    try { body = response.json(); } catch (_) { /* counted below */ }
  }
  const correctStatus = !transportFailed && response.status === expectedStatus;
  const correctIdentity = body !== null && (asynchronous
    ? body.command_id === item.command_id
    : body.delivery_id === item.delivery_id && typeof body.id === 'string' && uuidPattern.test(body.id));
  const correctBodyStatus = body !== null && body.status === expectedBodyStatus;
  const validBody = correctIdentity && correctBodyStatus;
  const success = correctStatus && validBody;
  check(null, {
    'expected HTTP status': function () { return correctStatus; },
    'matching response identity': function () { return correctIdentity; },
    'expected body status': function () { return correctBodyStatus; },
  });
  transportFailures.add(transportFailed ? 1 : 0);
  unexpectedStatuses.add(!transportFailed && !correctStatus ? 1 : 0);
  invalidBodies.add(!transportFailed && !validBody ? 1 : 0);
  planErrors.add(0);
  accepted.add(success ? 1 : 0);
  failed.add(success ? 0 : 1);
  returned.add(1);
}

export function handleSummary(data) {
  function count(name) {
    const metric = data.metrics[name];
    return metric && metric.values ? (metric.values.count || 0) : 0;
  }
  function singleSample(name) {
    const metric = data.metrics[name];
    return metric && metric.values && metric.values.count === 1 ? metric.values.avg : null;
  }
  const scenarioStartedMs = singleSample('diag_scenario_start_epoch_ms');
  const firstRequestMs = singleSample('diag_first_request_epoch_ms');
  const counts = {
    planned: expected,
    iterations_started: count('diag_iterations_started'),
    iterations_returned: count('diag_iterations_returned'),
    iterations_completed_builtin: count('iterations'),
    requests_attempted: count('diag_requests_started'),
    http_requests_builtin: count('http_reqs'),
    responses_received: count('diag_responses_received'),
    accepted: count('diag_accepted'),
    failed_iterations: count('diag_failed_iterations'),
    transport_failures: count('diag_transport_failures'),
    unexpected_statuses: count('diag_unexpected_statuses'),
    invalid_bodies: count('diag_invalid_bodies'),
    plan_errors: count('diag_plan_errors'),
    dropped_iterations: count('dropped_iterations'),
  };
  counts.started_but_not_returned = counts.iterations_started - counts.iterations_returned;
  data.admission_diagnostic = {
    methodology: 'constant_arrival_post_only_summary_after_http',
    configuration: {
      mode: config.mode,
      request_path: requestPath,
      expected_http_status: expectedStatus,
      target_requests_per_second: rate,
      submission_duration_seconds: durationSeconds,
      request_timeout_seconds: timeoutSeconds,
      preallocated_vus: 512,
      maximum_vus: 512,
      cpu_limit_to_enforce_externally: 4,
      plan_records: plan.length,
      single_local_instance_required: true,
    },
    timing: {
      scenario_started_epoch_ms: scenarioStartedMs,
      scenario_started_at: scenarioStartedMs === null ? null : new Date(scenarioStartedMs).toISOString(),
      submission_end_epoch_ms: scenarioStartedMs === null ? null : scenarioStartedMs + durationSeconds * 1000,
      first_request_epoch_ms: firstRequestMs,
      per_request_send_timestamps_recorded: false,
      index_schedule_estimate_valid: counts.dropped_iterations === 0
        && counts.requests_attempted === expected && counts.plan_errors === 0,
    },
    counts: counts,
    attempted_per_submission_second: counts.requests_attempted / durationSeconds,
    accepted_per_submission_second: counts.accepted / durationSeconds,
    notes: [
      'accepted means locally validated HTTP 201 for direct or HTTP 202 for events.',
      'accepted_per_submission_second includes responses returned during HTTP graceful drain.',
      'dropped_iterations counts arrivals not started for lack of free VUs; it is not Python generator_lag.',
      'iterationInTest indexes started iterations, not every scheduled arrival; no retries in this script.',
      'A returned iteration can have failed validation. A started-but-not-returned iteration was interrupted.',
      'http_req_duration excludes connection blocking/setup; diag_request_wall_ms includes them at millisecond precision.',
      'Scenario and first-request epoch markers are sampled once and written only in handleSummary.',
      'No individual send timestamps are recorded. Planned time from index/rate is only an estimate and is invalid with dropped arrivals.',
      'diag_start_offset_error_ms describes actual start minus scenario_start + index/rate; interpret as scheduling error only with no dropped arrivals.',
      'No raw request receipt or command_id metric tags are retained. Validate identities against plan and final DB snapshot.',
      'Passive drain, API/worker stop and database snapshot are the external runner responsibility.',
    ],
  };
  return { '/results/k6-summary.json': JSON.stringify(data, null, 2) + '\n' };
}
