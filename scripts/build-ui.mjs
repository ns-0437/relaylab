// Demo UI milestone: bundles ui/src/main.tsx (React + TypeScript) into
// ui/dist/app.js with esbuild -- a locally installed, lockfile-managed
// dependency, never a CDN. The built output has NO runtime dependency
// on network access to load React; it is served directly by
// src/api/server.ts from ui/dist/.
import * as esbuild from "esbuild";
import { readFile, writeFile, mkdir, copyFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const UI_DIR = path.join(HERE, "..", "ui");
const OUT_DIR = path.join(UI_DIR, "dist");

await mkdir(OUT_DIR, { recursive: true });

await esbuild.build({
  entryPoints: [path.join(UI_DIR, "src", "main.tsx")],
  bundle: true,
  outfile: path.join(OUT_DIR, "app.js"),
  format: "esm",
  target: "es2022",
  sourcemap: true,
  minify: process.env.NODE_ENV === "production",
  define: { "process.env.NODE_ENV": JSON.stringify(process.env.NODE_ENV ?? "development") },
});

// index.html references the bundled app.js (type="module") and has no
// CDN <script> tags at all -- copied as-is into dist/.
await copyFile(path.join(UI_DIR, "index.html"), path.join(OUT_DIR, "index.html"));
await copyFile(path.join(UI_DIR, "styles.css"), path.join(OUT_DIR, "styles.css"));

console.log("[build-ui] wrote ui/dist/app.js (+ .map), ui/dist/index.html, ui/dist/styles.css");
