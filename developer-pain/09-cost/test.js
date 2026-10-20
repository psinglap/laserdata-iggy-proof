const assert = require("assert");
const { INSTANCES, estimate } = require("./model.js");
const i4i = INSTANCES.find(x => x.id === "i4i.2xlarge"), m7i = INSTANCES.find(x => x.id === "m7i.xlarge");
const base = { writeMBps: 50, fanout: 3, retentionDays: 3, replicas: 3, zones: 3, instance: i4i,
  headroomPct: 30, crossZoneClientPct: 67, measuredMBpsPerNode: null };
let r = estimate(base);
assert.strictEqual(Math.round(r.storedGB), 38880);                  // 50 MB/s * 3 d * 3 copies
assert.strictEqual(r.nodes, Math.ceil(38880 / (1875 * 0.7)));       // storage binds: 30 nodes
assert.strictEqual(r.binding, "storage");
assert.strictEqual(r.throughputChecked, false);
r = estimate({ ...base, retentionDays: 0.25, measuredMBpsPerNode: 10 });
assert.strictEqual(r.nodes, 5); assert.strictEqual(r.binding, "throughput");
r = estimate({ ...base, zones: 1 });
assert.strictEqual(r.network, 0);
r = estimate({ ...base, instance: m7i });
assert.strictEqual(r.nodes, 3); assert.ok(r.storage > 0);
assert.ok(Math.abs(r.objectStoreRef - 38880 / 3 * 0.023) < 1e-6);
console.log("all model tests passed");
