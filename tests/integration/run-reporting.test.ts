// Demo UI milestone: factual lifecycle reporting through the real API
// server -- provider failure, failed verification followed by a retry,
// and cancellation after an already-published checkpoint. Simulated
// provider responses only (injected in-process); live mode forbidden.
process.env.RELAYLAB_MYSQL_DATABASE = process.env.RELAYLAB_TEST_MYSQL_DATABASE ?? "relaylab_test";
delete process.env.RELAYLAB_LIVE_MODE;

import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import type mysql from "mysql2/promise";
import Anthropic from "@anthropic-ai/sdk";
import { createApiServer, type ApiServerOptions } from "../../src/api/server.js";
import { simulatedClientReturning, type StageHookName } from "../../src/api/executor.js";
import { AnthropicAdapterError, type MessagesClient } from "../../src/ai/anthropic-client.js";
import { createPool } from "../../src/db/pool.js";
import { loadMysqlConfig } from "../../src/config.js";

const CORRECT_FIX = "function add(a, b) {\n  return a + b;\n}\n\nmodule.exports = { add };\n";
const WRONG_FIX = "function add(a, b) {\n  return a * b;\n}\n\nmodule.exports = { add };\n";

let admin: mysql.Pool;
before(() => {
  admin = createPool(loadMysqlConfig());
});
after(async () => {
  await admin.end();
});

async function withServer<T>(options: ApiServerOptions, body: (api: { post: (p: string, b: unknown) => Promise<{ status: number; body: any }> }) => Promise<T>): Promise<T> {
  const handle = await createApiServer({ forbidLiveMode: true, ...options });
  await new Promise<void>((resolve) => handle.server.listen(0, "127.0.0.1", resolve));
  const base = `http://127.0.0.1:${(handle.server.address() as AddressInfo).port}`;
  const post = (p: string, b: unknown) =>
    fetch(base + p, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(b) }).then(async (r) => ({ status: r.status, body: (await r.json()) as any }));
  try {
    return await body({ post });
  } finally {
    await (await handle.shutdown()).cleanup;
  }
}

async function row(runId: string): Promise<{ status: string; detail: any } | undefined> {
  const [rows] = await admin.query(`SELECT status, detail_json FROM demo_runs WHERE run_id = ?`, [runId]);
  const r = (rows as { status: string; detail_json: unknown }[])[0];
  return r && { status: r.status, detail: typeof r.detail_json === "string" ? JSON.parse(r.detail_json) : r.detail_json };
}

async function waitTerminal(runId: string): Promise<{ status: string; detail: any }> {
  for (let i = 0; i < 240; i++) {
    const r = await row(runId);
    if (r && ["completed", "failed", "cancelled", "interrupted"].includes(r.status)) return r;
    await new Promise((res) => setTimeout(res, 250));
  }
  throw new Error(`run ${runId} never reached a terminal status`);
}

test("provider failure on every attempt is reported as a failed GENERATION stage, with no verification outcome invented", async () => {
  const failing: MessagesClient = {
    messages: {
      create: (async () => {
        throw new AnthropicAdapterError("simulated provider outage (test)", null);
      }) as unknown,
    } as unknown as Anthropic.Messages,
  } as MessagesClient;

  await withServer({ executor: { simulatedClient: () => failing } }, async ({ post }) => {
    const started = await post("/api/runs", { mode: "ai-simulated" });
    assert.equal(started.status, 201);
    const final = await waitTerminal(started.body.runId);
    assert.equal(final.status, "failed");
    assert.equal(final.detail.stage, "generation", "no attempt reached verification, so the failure belongs to generation");
    assert.ok(final.detail.attempts.length >= 1);
    for (const a of final.detail.attempts) {
      assert.equal(a.outcome, "provider_error");
      assert.equal(a.verificationOutcome, undefined, "a provider failure never carries a verification outcome");
    }
    assert.equal(final.detail.checkpointDigest, undefined);
  });
});

test("a failed verification followed by a successful retry keeps the first attempt FAILED, and each transition is persisted while the run is still in progress", async () => {
  const observedAtSecondGeneration: { stage?: string; attempts?: any[] } = {};
  let runIdForProbe = "";
  let calls = 0;
  const flaky = (): MessagesClient => {
    const wrong = simulatedClientReturning(WRONG_FIX);
    const right = simulatedClientReturning(CORRECT_FIX);
    return {
      messages: {
        create: (async (...args: unknown[]) => {
          calls++;
          if (calls === 1) return (wrong.messages.create as (...a: unknown[]) => unknown)(...args);
          // Inside attempt 2's generation call -- the worker has NOT returned yet.
          const snapshot = await row(runIdForProbe);
          observedAtSecondGeneration.stage = snapshot?.detail.stage;
          observedAtSecondGeneration.attempts = snapshot?.detail.attempts;
          return (right.messages.create as (...a: unknown[]) => unknown)(...args);
        }) as unknown,
      } as unknown as Anthropic.Messages,
    } as MessagesClient;
  };

  let releaseCheckpoint!: () => void;
  const checkpointGate = new Promise<void>((r) => (releaseCheckpoint = r));
  let atCheckpoint!: () => void;
  const reachedCheckpoint = new Promise<void>((r) => (atCheckpoint = r));
  const beforeStage = async (stage: StageHookName, runId: string) => {
    runIdForProbe = runId;
    if (stage === "checkpoint") {
      atCheckpoint();
      await checkpointGate;
    }
  };

  await withServer({ executor: { simulatedClient: flaky, beforeStage } }, async ({ post }) => {
    const started = await post("/api/runs", { mode: "ai-simulated" });
    assert.equal(started.status, 201);
    await reachedCheckpoint;

    assert.equal(observedAtSecondGeneration.stage, "generation", "attempt 2's start was persisted before its generation call ran");
    assert.equal(observedAtSecondGeneration.attempts?.[0]?.verificationOutcome, "verification_failed", "attempt 1's failed verification was already durable mid-run");

    releaseCheckpoint();
    const final = await waitTerminal(started.body.runId);
    assert.equal(final.status, "completed");
    assert.equal(final.detail.attempts[0].verificationOutcome, "verification_failed", "the retry must never turn the earlier failed verification into a success");
    assert.equal(final.detail.attempts[1].verificationOutcome, "passed");
    assert.equal(final.detail.acknowledged, true);
    assert.ok(final.detail.checkpointDigest, "checkpoint success is backed by the durable digest");
  });
});

test("cancellation after the checkpoint is already published is reported as CANCELLED (not failed), preserving the checkpoint and starting no transfer", async () => {
  let releaseTransfer!: () => void;
  const transferGate = new Promise<void>((r) => (releaseTransfer = r));
  let atTransfer!: (runId: string) => void;
  const reachedTransfer = new Promise<string>((r) => (atTransfer = r));
  const beforeStage = async (stage: StageHookName, runId: string) => {
    if (stage === "transfer") {
      atTransfer(runId);
      await transferGate;
    }
  };

  await withServer({ executor: { beforeStage } }, async ({ post }) => {
    const started = await post("/api/runs", { mode: "deterministic" });
    const runId = await reachedTransfer;
    const before = await row(runId);
    assert.equal(before?.status, "checkpointed", "the checkpoint was published before the cancel");

    assert.equal((await post(`/api/runs/${runId}/cancel`, {})).status, 202);
    releaseTransfer();
    const final = await waitTerminal(started.body.runId);

    assert.equal(final.status, "cancelled", "a user cancel is reported distinctly from a failure");
    assert.equal(final.detail.checkpointDigest, before?.detail.checkpointDigest, "the already-published checkpoint stays reported");
    assert.equal(final.detail.resultCommit, before?.detail.resultCommit);
    const [handoffs] = await admin.query(`SELECT COUNT(*) AS c FROM task_handoffs WHERE task_id = (SELECT task_id FROM demo_runs WHERE run_id = ?)`, [runId]);
    assert.equal((handoffs as { c: number }[])[0]!.c, 0, "no ownership transfer was started after the cancel");
  });
});
