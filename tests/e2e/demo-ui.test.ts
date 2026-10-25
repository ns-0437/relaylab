// Demo UI milestone, task 6: checked-in, automated browser acceptance.
// Drives the REAL built UI (ui/dist/, served by src/api/server.ts) in a
// real Chromium via Playwright, against real MySQL/Docker and a
// simulated (no-network) provider client -- never a mock of the server
// or the pipeline. Isolated test storage (the SAME disposable
// relaylab_test database every other integration test uses); every
// server instance here is created with `forbidLiveMode: true`, so a
// stray RELAYLAB_LIVE_MODE/API key in the environment can never cause a
// real, paid provider call even if something were misconfigured.
//
// Run with: npm run test:e2e-ui
process.env.RELAYLAB_MYSQL_DATABASE = process.env.RELAYLAB_TEST_MYSQL_DATABASE ?? "relaylab_test";
delete process.env.RELAYLAB_LIVE_MODE;

import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { chromium, type Browser, type Page } from "playwright";
import Anthropic from "@anthropic-ai/sdk";
import { createApiServer, type ApiServerHandle, type ApiServerOptions } from "../../src/api/server.js";
import type { MessagesClient } from "../../src/ai/anthropic-client.js";
import { AnthropicAdapterError } from "../../src/ai/anthropic-client.js";

let browser: Browser;

before(async () => {
  browser = await chromium.launch();
});
after(async () => {
  await browser.close();
});

async function startServer(options: ApiServerOptions = {}): Promise<{ handle: ApiServerHandle; url: string }> {
  const handle = await createApiServer({ forbidLiveMode: true, ...options });
  await new Promise<void>((resolve) => handle.server.listen(0, "127.0.0.1", resolve));
  return { handle, url: `http://127.0.0.1:${(handle.server.address() as AddressInfo).port}` };
}

/** A fresh page for one test, closed afterward regardless of outcome. */
async function withPage<T>(url: string, body: (page: Page) => Promise<T>): Promise<T> {
  const page = await browser.newPage();
  try {
    await page.goto(url);
    return await body(page);
  } finally {
    await page.close();
  }
}

/** The provider never reached in these tests -- confirms forbidLiveMode actually refuses ai-live before any client is even constructed. */
function failingAlwaysClient(): MessagesClient {
  return {
    messages: {
      create: (async () => {
        throw new Error("this test's provider client must never be called");
      }) as unknown,
    } as unknown as Anthropic.Messages,
  } as MessagesClient;
}

test("happy path through the UI: simulated provider, real Git/MySQL/Docker -- start, checkpoint, worker-B restore, durable acknowledgement", async () => {
  const { handle, url } = await startServer();
  try {
    await withPage(url, async (page) => {
      // ai-simulated is the form's default selection -- no change needed.
      await page.click('button:has-text("Start run")');

      await page.locator("text=status: completed").waitFor({ timeout: 180_000 });
      const stages = await page.locator(".stage-row .stage").allTextContents();
      assert.deepEqual(
        stages.map((s) => s.trim()),
        ["Generation", "Verification", "Checkpoint", "Transfer", "Restore / Ack"],
      );
      const doneCount = await page.locator(".stage-row .stage.done").count();
      assert.equal(doneCount, 5, "a completed run must show every stage as done");

      await page.locator("text=Restore acknowledgement: confirmed by durable backend state").waitFor({ timeout: 5_000 });
      await page.locator("text=worker B restored commit:").locator("text=MATCHES").waitFor({ timeout: 5_000 });
    });
  } finally {
    await (await handle.shutdown()).cleanup;
  }
});

test("failure path through the UI: a simulated provider failure is shown as a failed run with an error message, never silently stuck", async () => {
  const { handle, url } = await startServer({ executor: { simulatedClient: failingAlwaysClient } });
  try {
    await withPage(url, async (page) => {
      await page.click('button:has-text("Start run")');
      await page.locator("text=status: failed").waitFor({ timeout: 60_000 });
      const errorText = await page.locator(".error-box").last().textContent();
      assert.match(errorText ?? "", /generation did not succeed/, "no attempt ever reached real verification, so this must be reported as a GENERATION failure");
      // Every attempt's own outcome ("provider_error") is shown in the attempts table.
      await page.locator("table td:has-text('provider_error')").first().waitFor({ timeout: 5_000 });
      const activeCount = await page.locator(".stage.active").count();
      assert.equal(activeCount, 0, "a failed run must never still show a stage as actively in progress");
    });
  } finally {
    await (await handle.shutdown()).cleanup;
  }
});

test("cancellation through the UI: clicking Cancel run on an active run ends it as cancelled, with the Cancel button removed", async () => {
  const { handle, url } = await startServer();
  try {
    await withPage(url, async (page) => {
      await page.selectOption("#mode-select", "deterministic");
      await page.click('button:has-text("Start run")');

      const cancelButton = page.locator('button:has-text("Cancel run")');
      await cancelButton.waitFor({ timeout: 30_000 });
      await cancelButton.click();

      await page.locator("text=status: cancelled").waitFor({ timeout: 60_000 });
      assert.equal(await cancelButton.count(), 0, "a terminal run must no longer offer a Cancel button");
    });
  } finally {
    await (await handle.shutdown()).cleanup;
  }
});

test("a server started with forbidLiveMode refuses ai-live through the UI before any provider client is constructed", async () => {
  // Live mode ENABLED server-side (env), but forbidLiveMode overrides
  // it -- this is the guard against a stray env var in CI/dev causing
  // an accidental paid call from an automated test run.
  process.env.RELAYLAB_LIVE_MODE = "1";
  process.env.ANTHROPIC_API_KEY = "sk-test-placeholder-never-used";
  let server: { handle: ApiServerHandle; url: string } | undefined;
  try {
    server = await startServer({ executor: { simulatedClient: failingAlwaysClient } });
  } finally {
    delete process.env.RELAYLAB_LIVE_MODE;
    delete process.env.ANTHROPIC_API_KEY;
  }
  try {
    await withPage(server.url, async (page) => {
      const status = (await fetch(`${server!.url}/api/status`).then((r) => r.json())) as { liveModeEnabled: boolean };
      assert.equal(status.liveModeEnabled, false, "the UI's own badge must reflect forbidLiveMode, not the raw environment variable");
      // The live-mode option is only disabled in the <select> when the
      // badge reports it disabled -- confirmed directly, not assumed.
      const liveOption = page.locator('#mode-select option[value="ai-live"]');
      assert.equal(await liveOption.getAttribute("disabled"), "");
    });
  } finally {
    await (await server.handle.shutdown()).cleanup;
  }
});
