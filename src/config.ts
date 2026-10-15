// Centralized env reading. Never logs credential values.

export interface MysqlConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

export function loadMysqlConfig(): MysqlConfig {
  return {
    host: process.env.RELAYLAB_MYSQL_HOST ?? "127.0.0.1",
    port: Number(process.env.RELAYLAB_MYSQL_PORT ?? 3306),
    user: process.env.RELAYLAB_MYSQL_USER ?? "relaylab_dev",
    password: process.env.RELAYLAB_MYSQL_PASSWORD ?? "relaylab_dev_password",
    database: process.env.RELAYLAB_MYSQL_DATABASE ?? "relaylab",
  };
}

/**
 * A SEPARATE, disposable database for integration tests (created by
 * infra/mysql-init/01-test-database.sql), distinct from the "relaylab" dev
 * database `npm run migrate` / the publisher / consumer / demo scripts use
 * by default. tests/integration/helpers.ts uses this, never loadMysqlConfig,
 * so test truncation can never touch dev data.
 */
export function loadTestMysqlConfig(): MysqlConfig {
  return { ...loadMysqlConfig(), database: process.env.RELAYLAB_TEST_MYSQL_DATABASE ?? "relaylab_test" };
}

export function loadNatsConfig(): { servers: string } {
  return { servers: process.env.RELAYLAB_NATS_URL ?? "127.0.0.1:24222" };
}

export const DEFAULT_LEASE_MS = Number(process.env.RELAYLAB_LEASE_MS ?? 15_000);

/**
 * NATS stream/subject/durable-consumer names are namespaced so integration
 * tests never share a stream with a running dev publisher/consumer/demo.
 * Reads the env var at call time (not module load time) so a test process
 * that sets RELAYLAB_NAMESPACE before calling these picks it up reliably
 * regardless of import order.
 */
function namespace(): string {
  return process.env.RELAYLAB_NAMESPACE ?? "relaylab";
}

export function eventsSubject(): string {
  return `${namespace()}.events`;
}

export function eventsStream(): string {
  return namespace().toUpperCase().replace(/[^A-Z0-9_]/g, "_") + "_EVENTS";
}

/**
 * Both the NATS durable consumer name AND the consumer_name partition key
 * in task_consumer_state / consumer_processed -- see DEFAULT_CONSUMER_OPTIONS
 * in src/messaging/consumer.ts, which is the single source of truth for a
 * running consumer process's identity.
 */
export function eventsDurable(): string {
  return process.env.RELAYLAB_CONSUMER_NAME ?? `${namespace()}-durable-consumer`;
}
