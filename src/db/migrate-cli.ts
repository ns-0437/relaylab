// CLI entrypoint for `npm run migrate`. Kept separate from migrate.ts so
// that importing applyMigrations() (as tests/integration/helpers.ts does)
// never has the side effect of running the CLI's main().
import { applyMigrations } from "./migrate.js";
import { loadMysqlConfig } from "../config.js";

applyMigrations(loadMysqlConfig()).catch((err) => {
  console.error("Migration failed:", err instanceof Error ? err.message : err);
  process.exitCode = 1;
});
