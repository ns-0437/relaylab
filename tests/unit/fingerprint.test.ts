import { test } from "node:test";
import assert from "node:assert/strict";
import { fingerprint } from "../../src/domain/fingerprint.js";

test("fingerprint is independent of object key order", () => {
  const a = fingerprint({ type: "Checkpoint", digest: "abc", nested: { x: 1, y: 2 } });
  const b = fingerprint({ nested: { y: 2, x: 1 }, digest: "abc", type: "Checkpoint" });
  assert.equal(a, b);
});

test("fingerprint differs for different payloads", () => {
  const a = fingerprint({ type: "Checkpoint", digest: "abc" });
  const b = fingerprint({ type: "Checkpoint", digest: "def" });
  assert.notEqual(a, b);
});

test("fingerprint preserves array order as meaningful", () => {
  const a = fingerprint({ items: [1, 2, 3] });
  const b = fingerprint({ items: [3, 2, 1] });
  assert.notEqual(a, b);
});
