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
const deliveryId = '00000000-0000-4000-8000-000000000001';
const returnId = '00000000-0000-4000-8000-000000000002';
const pending = { id: deliveryId, status: 'pending' };

function response(status, payload = {}, error_code = 0) {
  return { status, error_code, timings: { duration: 1 }, json: () => payload };
}

function scenario(responses, name = 'create') {
  const counts = {};
  const labels = [];
  const receipts = [];
  const queue = [...responses];
  const context = vm.createContext({
    __ENV: { SCENARIO: name },
    Counter: class {
      constructor(name) { this.name = name; }
      add(value, tags) {
        counts[this.name] = (counts[this.name] || 0) + value;
        if (tags) labels.push(tags.name);
      }
    },
    Trend: class { add() {} },
    http: { request() { assert.ok(queue.length, 'unexpected HTTP call'); return queue.shift(); } },
    check: (value, checks) => Object.values(checks).every(check => check(value)),
    execution: { scenario: { iterationInTest: 0 } },
    console: { log: message => receipts.push(JSON.parse(message.slice('RECEIPT '.length))) },
  });
  vm.runInContext(source, context);
  vm.runInContext('runWorkflow()', context);
  assert.equal(queue.length, 0, 'all expected HTTP calls were made');
  return { counts, labels, receipts };
}

test('valid workflow counts four successful requests and initializes all failure counters', () => {
  const { counts, labels, receipts } = scenario([
    response(201, pending),
    response(200, { id: deliveryId, status: 'delivered' }),
    response(201, { id: returnId, delivery_id: deliveryId, status: 'requested' }),
    response(200, { id: returnId, delivery_id: deliveryId, status: 'requested' }),
  ], 'flow');
  assert.equal(counts.workflows_attempted, 1);
  assert.equal(counts.workflows_completed, 1);
  assert.equal(counts.http_requests_total, 4);
  assert.equal(counts.http_requests_succeeded, 4);
  for (const name of ['http_requests_failed', 'http_responses_4xx', 'http_responses_5xx',
    'http_transport_errors', 'http_timeouts', 'http_unexpected_responses',
    'technical_errors', 'unknown_writes']) assert.equal(counts[name], 0, name);
  assert.ok(labels.every(label => !label.includes(deliveryId) && !label.includes(returnId)));
  assert.deepEqual(receipts, [{ delivery_id: deliveryId, delivered_id: deliveryId,
    return_id: returnId, return_delivery_id: deliveryId }]);
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
  test(`${name} belongs to one failure category; only transport timeouts count as timeouts`, () => {
    const { counts, receipts } = scenario([result]);
    assert.equal(counts.workflows_attempted, 1);
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
    assert.deepEqual(receipts, []);
  });
}

test('failed read counts as transport failure without an unknown write', () => {
  const { counts } = scenario([response(0, {}, 1050)], 'read');
  assert.equal(counts.http_requests_failed, 1);
  assert.equal(counts.http_transport_errors, 1);
  assert.equal(counts.http_timeouts, 1);
  assert.equal(counts.unknown_writes, 0);
});

test('a failure midway through a workflow preserves confirmed writes and stops later HTTP calls', () => {
  const { counts, receipts } = scenario([response(201, pending), response(503)], 'flow');
  assert.equal(counts.workflows_attempted, 1);
  assert.equal(counts.workflows_completed, 0);
  assert.equal(counts.http_requests_total, 2);
  assert.equal(counts.http_requests_succeeded, 1);
  assert.equal(counts.http_requests_failed, 1);
  assert.equal(counts.unknown_writes, 1);
  assert.deepEqual(receipts, [{ delivery_id: deliveryId }]);
});
