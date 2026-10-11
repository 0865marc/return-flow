// Optional, dependency-free checks: node --test benchmarks/tests/load.test.mjs
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

// Execute the real scenario, replacing only k6's module boundary with test doubles.
const source = readFileSync(new URL('../scenarios/load.js', import.meta.url), 'utf8')
  .replace(/^import .*;\n/gm, '')
  .replace('export default function ()', 'function runWorkflow()')
  .replace(/export (const|function) /g, '$1 ');
const deliveryId = '11111111-1111-4111-8111-111111111111';
const returnId = '22222222-2222-4222-8222-222222222222';
const pending = { id: deliveryId, status: 'pending' };
const delivered = { id: deliveryId, status: 'delivered' };
const returned = { id: returnId, delivery_id: deliveryId, status: 'requested' };
const startedAt = 1700000000000;
const commandId = index => `00000000-0000-4000-8000-${String(index).padStart(12, '0')}`;
const eventDriven = { VARIANT: 'event_driven' };

function response(status, payload = {}, error_code = 0) {
  return { status, error_code, timings: { duration: 1 }, json: () => payload };
}
function accepted(index = 2) {
  return response(202, { command_id: commandId(index), status: 'accepted' });
}
function command(status, result = null, index = 2, type = 'create_delivery', error = null) {
  return response(200, { command_id: commandId(index), type, status, result, error });
}
function dependencyResponses() {
  return [accepted(), command('completed', pending), accepted(3),
    command('completed', delivered, 3, 'ensure_delivery_delivered')];
}

function scenario(responses, environment = {}, run = true) {
  const counts = {};
  const labels = [];
  const receipts = [];
  const calls = [];
  const trends = {};
  const sleeps = [];
  let clock = startedAt;
  let commandSequence = 0;
  const queue = [...responses];
  const context = vm.createContext({
    __ENV: { ...environment },
    Counter: class {
      constructor(name) { this.name = name; }
      add(value, tags) {
        counts[this.name] = (counts[this.name] || 0) + value;
        if (tags) labels.push(tags.name);
      }
    },
    Trend: class {
      constructor(name) { this.name = name; }
      add(value) { (trends[this.name] ||= []).push(value); }
    },
    http: {
      request(method, url, body, options) {
        assert.ok(queue.length, 'unexpected HTTP call');
        calls.push({ method, url, body, options });
        const result = queue.shift();
        clock += result.elapsed ?? result.timings.duration;
        return result;
      },
    },
    crypto: {
      randomBytes(size) {
        const bytes = new Uint8Array(size);
        bytes[size - 1] = ++commandSequence;
        return bytes.buffer;
      },
    },
    Date: class extends Date { static now() { return clock; } },
    sleep(seconds) { sleeps.push(seconds); clock += seconds * 1000; },
    check: (value, checks) => Object.values(checks).every(check => check(value)),
    execution: { scenario: { startTime: startedAt, iterationInTest: 0 } },
    console: { log: message => receipts.push(JSON.parse(message.slice('RECEIPT '.length))) },
  });
  vm.runInContext(source, context);
  if (run) {
    vm.runInContext('runWorkflow()', context);
    assert.equal(queue.length, 0, 'all expected HTTP calls were made');
    // Failed attempts must be auditable even when the first POST never succeeds.
    assert.equal(receipts.length, 1);
    assert.equal(receipts[0].flow_id, commandId(1));
    assert.equal(receipts[0].started_at_ms, startedAt);
    assert.equal(receipts[0].finished_at_ms, clock);
    assert.ok(receipts[0].finished_at_ms >= receipts[0].started_at_ms);
  }
  return { counts, labels, receipts, calls, trends, sleeps,
    options: JSON.parse(vm.runInContext('JSON.stringify(options)', context)),
    summary(data) {
      return JSON.parse(vm.runInContext(
        `JSON.stringify(handleSummary(${JSON.stringify(data)}))`, context));
    },
  };
}

function auditFields(receipt) {
  const { flow_id, started_at_ms, finished_at_ms, ...fields } = receipt;
  return fields;
}

test('direct full flow performs exactly three writes and no verification reads', () => {
  const { counts, labels, receipts, calls, trends, options } = scenario([
    response(201, pending), response(200, delivered), response(201, returned),
  ]);
  assert.equal(counts.workflows_attempted, 1);
  assert.equal(counts.workflows_submitted, 1);
  assert.equal(counts.workflows_completed, 1);
  assert.equal(counts.http_requests_total, 3);
  assert.equal(counts.http_requests_succeeded, 3);
  for (const name of ['http_requests_failed', 'http_responses_4xx', 'http_responses_5xx',
    'http_transport_errors', 'http_timeouts', 'http_unexpected_responses',
    'technical_errors', 'unknown_writes']) assert.equal(counts[name], 0, name);
  assert.deepEqual(calls.map(call => call.method), ['POST', 'POST', 'POST']);
  assert.deepEqual(calls.map(call => call.url), ['http://api:8000/deliveries',
    `http://api:8000/deliveries/${deliveryId}/deliver`, 'http://api:8000/returns']);
  assert.equal(calls[2].body, JSON.stringify({ delivery_id: deliveryId }));
  assert.deepEqual(calls.map(call => call.options.headers['Idempotency-Key']),
    [commandId(2), commandId(3), commandId(4)]);
  assert.equal(counts.commands_submitted, 3);
  assert.equal(counts.commands_completed, 3);
  assert.deepEqual(auditFields(receipts[0]), {
    command_ids: [commandId(2), commandId(3), commandId(4)],
    accepted_command_ids: [commandId(2), commandId(3), commandId(4)],
    delivery_id: deliveryId, delivered_id: deliveryId, return_attempted: true, return_id: returnId,
    return_delivery_id: deliveryId });
  assert.deepEqual(trends.workflow_duration, [3]);
  assert.deepEqual(trends.load_started_at, [startedAt]);
  assert.equal(options.scenarios.workload.gracefulStop, '25s');
  assert.ok(labels.every(label => !label.includes(deliveryId) && !label.includes(returnId)));
});

const cases = [
  ['4xx', response(422), 'http_responses_4xx', 0, 0],
  ['5xx', response(503), 'http_responses_5xx', 0, 1],
  ['request timeout', response(0, {}, 1050), 'http_transport_errors', 1, 1],
  ['connection timeout', response(0, {}, 1211), 'http_transport_errors', 1, 1],
  ['connection refused', response(0, {}, 1212), 'http_transport_errors', 0, 1],
  ['TLS error', response(0, {}, 1300), 'http_transport_errors', 0, 1],
  ['invalid success body', response(201, { status: 'wrong' }), 'http_unexpected_responses', 0, 1],
  ['malformed JSON', { ...response(201), json() { throw new Error('invalid JSON'); } },
    'http_unexpected_responses', 0, 1],
  ['redirect', response(302), 'http_unexpected_responses', 0, 0],
];
for (const [name, result, category, timeouts, unknown] of cases) {
  test(`${name} records an auditable failed flow in exactly one HTTP failure category`, () => {
    const { counts, receipts, trends } = scenario([result]);
    assert.equal(counts.workflows_attempted, 1);
    assert.equal(counts.workflows_submitted, 0);
    assert.equal(counts.workflows_completed, 0);
    assert.equal(counts.http_requests_total, 1);
    assert.equal(counts.http_requests_succeeded, 0);
    assert.equal(counts.http_requests_failed, 1);
    assert.equal(counts.technical_errors, 1);
    for (const name of ['http_responses_4xx', 'http_responses_5xx',
      'http_transport_errors', 'http_unexpected_responses']) {
      assert.equal(counts[name], name === category ? 1 : 0, name);
    }
    assert.equal(counts.http_timeouts, timeouts);
    assert.equal(counts.unknown_writes, unknown);
    assert.equal(trends.workflow_duration, undefined);
    assert.deepEqual(auditFields(receipts[0]), {
      command_ids: [commandId(2)], accepted_command_ids: [],
    });
  });
}

test('a failure midway preserves confirmed writes and stops later HTTP calls', () => {
  const { counts, receipts } = scenario([response(201, pending), response(503)]);
  assert.equal(counts.workflows_completed, 0);
  assert.equal(counts.http_requests_total, 2);
  assert.equal(counts.http_requests_succeeded, 1);
  assert.equal(counts.http_requests_failed, 1);
  assert.equal(counts.unknown_writes, 1);
  assert.deepEqual(auditFields(receipts[0]), { delivery_id: deliveryId,
    command_ids: [commandId(2), commandId(3)], accepted_command_ids: [commandId(2)],
  });
});

test('direct final HTTP 503 preserves the return attempt for later database reconciliation', () => {
  const { counts, receipts, calls } = scenario([
    response(201, pending), response(200, delivered), response(503),
  ]);
  assert.equal(calls.length, 3);
  assert.equal(counts.workflows_submitted, 0);
  assert.equal(counts.workflows_completed, 0);
  assert.equal(counts.returns_created, 0);
  assert.equal(counts.unknown_writes, 1);
  assert.equal(counts.http_responses_5xx, 1);
  assert.deepEqual(auditFields(receipts[0]), {
    command_ids: [commandId(2), commandId(3), commandId(4)],
    accepted_command_ids: [commandId(2), commandId(3)],
    delivery_id: deliveryId, delivered_id: deliveryId, return_attempted: true,
  });
});

test('event flow observes only two dependencies; final acceptance never counts as completion', () => {
  const { counts, receipts, calls, trends, options, labels } = scenario([
    ...dependencyResponses(), accepted(4),
  ], eventDriven);
  assert.equal(counts.workflows_submitted, 1);
  assert.equal(counts.workflows_completed, 0);
  assert.equal(counts.http_requests_succeeded, 5);
  assert.equal(counts.commands_submitted, 3);
  assert.equal(counts.commands_completed, 2);
  assert.equal(counts.deliveries_created, 1);
  assert.equal(counts.deliveries_delivered, 1);
  assert.equal(counts.returns_created, 0);
  assert.deepEqual(auditFields(receipts[0]), {
    command_ids: [commandId(2), commandId(3), commandId(4)],
    accepted_command_ids: [commandId(2), commandId(3), commandId(4)],
    delivery_id: deliveryId, delivered_id: deliveryId, return_attempted: true,
  });
  assert.deepEqual(calls.map(call => call.method), ['POST', 'GET', 'POST', 'GET', 'POST']);
  for (const [index, position] of [0, 2, 4].entries()) {
    assert.equal(calls[position].options.headers['Idempotency-Key'], commandId(index + 2));
    if (position < 4) {
      assert.equal(calls[position + 1].url, `http://api:8000/commands/${commandId(index + 2)}`);
      assert.equal(calls[position + 1].options.headers['Idempotency-Key'], undefined);
    }
  }
  assert.equal(calls[0].url, 'http://api:8000/deliveries-async');
  assert.equal(calls[2].url, `http://api:8000/deliveries/${deliveryId}/deliver-async`);
  assert.equal(calls[4].url, 'http://api:8000/returns-async');
  assert.equal(calls[4].body, JSON.stringify({ delivery_id: deliveryId }));
  assert.equal(trends.workflow_duration, undefined);
  assert.deepEqual(trends.create_return_async_duration, [1]);
  assert.equal(options.scenarios.workload.gracefulStop, '45s');
  assert.ok(!options.systemTags.includes('url'));
  assert.ok(labels.every(label => !label.includes(deliveryId) && !label.includes(returnId)
    && !label.includes(commandId(2))));
  assert.ok(calls.every(call => Object.keys(call.options.tags).join() === 'name'));
  assert.deepEqual(options.thresholds.workflows_submitted, ['count>0']);
  assert.equal(options.thresholds.workflows_completed, undefined);
});

test('final publication HTTP 503 retains all attempted IDs for the post-run outcome audit', () => {
  const { counts, receipts, calls } = scenario([
    ...dependencyResponses(), response(503),
  ], eventDriven);
  assert.equal(counts.workflows_submitted, 0);
  assert.equal(counts.workflows_completed, 0);
  assert.equal(counts.commands_submitted, 3);
  assert.equal(counts.commands_completed, 2);
  assert.equal(counts.unknown_writes, 1);
  assert.equal(counts.http_responses_5xx, 1);
  assert.equal(calls.length, 5);
  assert.equal(receipts[0].return_attempted, true);
  assert.deepEqual(auditFields(receipts[0]), {
    command_ids: [commandId(2), commandId(3), commandId(4)],
    accepted_command_ids: [commandId(2), commandId(3)],
    delivery_id: deliveryId, delivered_id: deliveryId, return_attempted: true,
  });
});

test('pending dependencies use exponential backoff capped by the configured interval', () => {
  const { counts, sleeps, receipts } = scenario([
    accepted(), command('pending'), command('pending'), command('pending'),
    command('pending'), command('completed', pending),
    accepted(3), command('completed', delivered, 3, 'ensure_delivery_delivered'), accepted(4),
  ], { ...eventDriven, POLL_INTERVAL: '0.05', POLL_MAX_INTERVAL: '0.15' });
  assert.deepEqual(sleeps, [0.05, 0.1, 0.15, 0.15]);
  assert.equal(counts.commands_completed, 2);
  assert.equal(counts.commands_timed_out, 0);
  assert.equal(counts.workflows_submitted, 1);
  assert.equal(counts.workflows_completed, 0);
  assert.equal(receipts[0].finished_at_ms - receipts[0].started_at_ms, 459);
});

test('pending deadline bounds request and sleep times without retrying the POST', () => {
  const { counts, calls, receipts, trends, sleeps } = scenario([
    accepted(), command('pending'), command('pending'),
  ], { ...eventDriven, COMMAND_TIMEOUT: '0.15', POLL_INTERVAL: '0.1' });
  assert.equal(counts.commands_submitted, 1);
  assert.equal(counts.commands_completed, 0);
  assert.equal(counts.commands_timed_out, 1);
  assert.equal(counts.workflows_completed, 0);
  assert.equal(counts.unknown_writes, 1);
  assert.equal(counts.technical_errors, 1);
  assert.equal(calls[1].options.timeout, 150);
  assert.equal(calls[2].options.timeout, 49);
  assert.deepEqual(sleeps, [0.1, 0.048]);
  assert.equal(receipts[0].finished_at_ms - receipts[0].started_at_ms, 151);
  assert.equal(calls.filter(call => call.method === 'POST').length, 1);
  assert.equal(trends.workflow_duration, undefined);
  assert.deepEqual(auditFields(receipts[0]), {
    command_ids: [commandId(2)], accepted_command_ids: [commandId(2)],
  });
});

test('terminal failed command is known and does not count as an unknown write', () => {
  const { counts, receipts } = scenario([
    accepted(), command('failed', null, 2, 'create_delivery', 'Business rule violation'),
  ], eventDriven);
  assert.equal(counts.commands_failed, 1);
  assert.equal(counts.commands_completed, 0);
  assert.equal(counts.commands_timed_out, 0);
  assert.equal(counts.deliveries_created, 0);
  assert.equal(counts.workflows_completed, 0);
  assert.equal(counts.unknown_writes, 0);
  assert.equal(counts.technical_errors, 1);
  assert.equal(counts.http_requests_succeeded, 2);
  assert.deepEqual(auditFields(receipts[0]), {
    command_ids: [commandId(2)], accepted_command_ids: [commandId(2)],
  });
});

test('publication timeout retains its key and never retries or assumes acceptance', () => {
  const { counts, calls, receipts } = scenario([response(0, {}, 1050)], eventDriven);
  assert.equal(counts.commands_submitted, 1);
  assert.equal(counts.commands_completed, 0);
  assert.equal(counts.http_timeouts, 1);
  assert.equal(counts.unknown_writes, 1);
  assert.equal(counts.workflows_completed, 0);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].options.headers['Idempotency-Key'], commandId(2));
  assert.deepEqual(auditFields(receipts[0]), {
    command_ids: [commandId(2)], accepted_command_ids: [],
  });
});

test('a later pending command preserves earlier confirmed writes and both attempted IDs', () => {
  const { counts, receipts } = scenario([
    accepted(), command('completed', pending),
    accepted(3), command('pending', null, 3, 'ensure_delivery_delivered'),
  ], { ...eventDriven, COMMAND_TIMEOUT: '0.05' });
  assert.equal(counts.commands_submitted, 2);
  assert.equal(counts.commands_completed, 1);
  assert.equal(counts.commands_timed_out, 1);
  assert.equal(counts.deliveries_created, 1);
  assert.equal(counts.deliveries_delivered, 0);
  assert.equal(counts.returns_created, 0);
  assert.equal(counts.unknown_writes, 1);
  assert.deepEqual(auditFields(receipts[0]), {
    command_ids: [commandId(2), commandId(3)],
    accepted_command_ids: [commandId(2), commandId(3)], delivery_id: deliveryId,
  });
});

for (const [name, invalid] of [
  ['wrong accepted command ID', response(202, { command_id: commandId(9), status: 'accepted' })],
  ['wrong acceptance state', response(202, { command_id: commandId(2), status: 'completed' })],
]) {
  test(`${name} fails before polling and retains uncertainty`, () => {
    const { counts, receipts } = scenario([invalid], eventDriven);
    assert.equal(counts.commands_submitted, 1);
    assert.equal(counts.commands_completed, 0);
    assert.equal(counts.unknown_writes, 1);
    assert.equal(counts.http_unexpected_responses, 1);
    assert.deepEqual(auditFields(receipts[0]), {
      command_ids: [commandId(2)], accepted_command_ids: [],
    });
  });
}

for (const [name, invalid, timeouts] of [
  ['malformed result', command('completed', { status: 'pending' }), 0],
  ['wrong command ID', command('completed', pending, 9), 0],
  ['wrong command type', command('completed', pending, 2, 'request_return'), 0],
  ['wrong command status', command('accepted'), 0],
  ['invalid terminal invariant', command('completed', pending, 2, 'create_delivery', 'error'), 0],
  ['polling server error', response(503), 0],
  ['polling transport timeout', response(0, {}, 1050), 1],
]) {
  test(`${name} after acceptance stops the flow with unknown business outcome`, () => {
    const { counts, receipts } = scenario([accepted(), invalid], eventDriven);
    assert.equal(counts.commands_submitted, 1);
    assert.equal(counts.commands_completed, 0);
    assert.equal(counts.commands_failed, 0);
    assert.equal(counts.commands_timed_out, timeouts);
    assert.equal(counts.technical_errors, 1);
    assert.equal(counts.unknown_writes, 1);
    assert.equal(counts.workflows_completed, 0);
    assert.equal(counts.deliveries_created, 0);
    assert.deepEqual(auditFields(receipts[0]), {
      command_ids: [commandId(2)], accepted_command_ids: [commandId(2)],
    });
  });
}

for (const [environment, message] of [
  [{ SCENARIO: 'read' }, /Only complete/],
  [{ SCENARIO: 'create' }, /Only complete/],
  [{ RATE: '0' }, /RATE must be a positive integer/],
  [{ RATE: '1.5' }, /RATE must be a positive integer/],
  [{ MAX_VUS: '1', PREALLOCATED_VUS: '2' }, /MAX_VUS must be >=/],
  [{ PREALLOCATED_VUS: '-1' }, /PREALLOCATED_VUS must be/],
  [{ COMMAND_TIMEOUT: '0' }, /COMMAND_TIMEOUT must be/],
  [{ POLL_INTERVAL: 'NaN' }, /POLL_INTERVAL must be/],
  [{ POLL_MAX_INTERVAL: '0.01', POLL_INTERVAL: '0.1' }, /POLL_MAX_INTERVAL must be >=/],
  [{ DURATION: '1m' }, /DURATION must be/],
  [{ DURATION: '0s' }, /DURATION must be/],
  [{ REQUEST_TIMEOUT: 'Infinitys' }, /REQUEST_TIMEOUT must be/],
]) {
  test(`invalid configuration ${JSON.stringify(environment)} fails before issuing requests`, () => {
    assert.throws(() => scenario([], environment, false), message);
  });
}

test('summary v5 preserves offered-load time window and reports submission separately', () => {
  const { summary } = scenario([], {
    ...eventDriven, DURATION: '12.5s', SUMMARY_PATH: '/tmp/full-flow.json',
  }, false);
  const output = summary({ metrics: {
    load_started_at: { values: { min: startedAt, max: startedAt } },
    workflows_attempted: { values: { count: 4 } },
    workflows_submitted: { values: { count: 3 } },
    workflows_completed: { values: { count: 0 } },
  } });
  const data = JSON.parse(output['/tmp/full-flow.json']);
  assert.deepEqual(data.benchmark, { version: 5, scenario: 'flow', variant: 'event_driven',
    started_at_ms: startedAt, offered_duration_ms: 12500 });
  assert.match(output.stdout, /3\/4 submitted; E2E completion is audited after the run/);
  assert.match(output.stdout, /workflow ms p50=n\/a/);
});

test('summary missing a load start retains null rather than inventing an audit window', () => {
  const { summary } = scenario([], { VARIANT: 'postgres_pool' }, false);
  const output = summary({ metrics: {} });
  const data = JSON.parse(output['/results/summary.json']);
  assert.equal(data.benchmark.started_at_ms, null);
  assert.equal(data.benchmark.offered_duration_ms, 30000);
  assert.match(output.stdout, /0\/0 submitted/);
});
