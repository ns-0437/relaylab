import { execFileSync } from 'node:child_process';
import { readFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const files = execFileSync('git', ['ls-files', '-z'], { cwd: root, encoding: 'utf8' }).split('\0').filter(Boolean);
const issues = [];
const forbiddenParts = new Set(['node_modules', 'dist', '.kubeconfig', '.secrets.env', '.port-forward-pids']);
const patterns = [
  /\bgh[pousr]_[A-Za-z0-9]{30,}\b/,
  /\bgithub_pat_[A-Za-z0-9_]{40,}\b/,
  /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,
  /^\s*(?:client-key-data|client-certificate-data):\s*\S+/m,
];
for (const relative of files) {
  const base = path.posix.basename(relative);
  if (relative.split('/').some(p => forbiddenParts.has(p)) ||
      (base.startsWith('.env') && base !== '.env.example') ||
      /\.(pem|key|p12|pfx)$/.test(base) ||
      relative.startsWith('infra/otel-evidence/') && base.endsWith('.jsonl')) {
    issues.push(`Generated or credential-bearing path is tracked: ${relative}`);
  }
  const content = readFileSync(path.join(root, relative), 'utf8');
  if (patterns.some(p => p.test(content))) issues.push(`Potential credential material in ${relative} (value suppressed)`);
}

// Check local README destinations; external URLs and anchors are deliberately excluded.
const readme = readFileSync(path.join(root, 'README.md'), 'utf8');
const destinations = [
  ...Array.from(readme.matchAll(/\]\(([^)]+)\)/g), m => m[1]),
  ...Array.from(readme.matchAll(/(?:href|src)="([^"]+)"/g), m => m[1]),
];
for (const destination of destinations) {
  if (/^(?:https?:|#|mailto:)/.test(destination)) continue;
  const local = decodeURIComponent(destination.split('#')[0]);
  if (!existsSync(path.join(root, local))) issues.push(`Broken README destination: ${destination}`);
}
if (issues.length) {
  for (const issue of issues) console.error(issue);
  process.exitCode = 1;
} else {
  console.log(`Publication checks passed: ${files.length} tracked paths; README local destinations resolve.`);
  console.log('This heuristic check is not a comprehensive secret scanner or a security certification.');
}
