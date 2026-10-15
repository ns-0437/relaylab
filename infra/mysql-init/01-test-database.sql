-- Runs automatically on first container creation (docker-entrypoint-initdb.d).
-- Creates a SEPARATE, disposable database for integration tests, distinct
-- from the "relaylab" dev database that `npm run migrate` / the
-- publisher/consumer/demo scripts use by default. This is what lets
-- `npm run test:integration` truncate tables freely without touching
-- anything running against the dev database.
CREATE DATABASE IF NOT EXISTS relaylab_test;
GRANT ALL PRIVILEGES ON relaylab_test.* TO 'relaylab_dev'@'%';
FLUSH PRIVILEGES;
