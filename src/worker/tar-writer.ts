// Phase 2C correctness patch, Part 3: a minimal, dependency-free USTAR
// writer AND reader (`readTar` added for the input-integrity check,
// Phase 3A Part A -- see docker-verify.ts). Exists for exactly one
// reason -- `git archive` cannot be
// used to build the verification input tar, because it ALWAYS applies
// `.gitattributes` `export-ignore`/`export-subst` when present, which
// is the OPPOSITE of what "exact-tree verification" requires (a
// tracked file's PRESENCE and BYTES in the verification input must
// match its Git object store content exactly, never a filtered/
// transformed archive-time view -- see docs/EXECUTION.md "Archive-
// attribute handling"). `src/worker/docker-verify.ts`'s
// `materializeCandidateInput` reads each tracked blob's raw bytes
// directly via `git cat-file -p` (src/worker/git.ts, unaffected by
// those attributes) and hands them to `writeTar` here to package.
//
// Deliberately NOT a general-purpose tar library: writes ONLY plain
// files (typeflag '0') with a mode and content, in USTAR format, with
// basic prefix-splitting for long paths (POSIX ustar's own, standard
// extension) -- nothing else (no directories as their own entries,
// no symlinks, no extended/pax headers). Anything this cannot
// represent is rejected explicitly by the caller before this is ever
// called (see `UNSUPPORTED_TREE_MODES` in docker-verify.ts), never
// silently dropped or corrupted here.
import { readFile, writeFile } from "node:fs/promises";

const BLOCK_SIZE = 512;
const NAME_MAX = 100;
const PREFIX_MAX = 155;

export interface TarEntry {
  /** Forward-slash-separated relative path. */
  readonly path: string;
  /** POSIX file mode, e.g. 0o644 or 0o755 -- the low 12 bits actually written are what matters for exec-bit fidelity. */
  readonly mode: number;
  readonly content: Buffer;
}

function octalField(value: number, fieldLength: number): Buffer {
  // fieldLength INCLUDES the trailing NUL -- the numeric digits occupy fieldLength - 1 bytes.
  const digits = fieldLength - 1;
  const text = value.toString(8).padStart(digits, "0");
  if (text.length > digits) {
    throw new Error(`tar-writer: value ${value} does not fit in a ${digits}-digit octal field`);
  }
  const buf = Buffer.alloc(fieldLength);
  buf.write(text, 0, "ascii");
  return buf;
}

function asciiField(value: string, fieldLength: number): Buffer {
  const encoded = Buffer.from(value, "utf8");
  if (encoded.length > fieldLength) {
    throw new Error(`tar-writer: value "${value}" (${encoded.length} bytes) does not fit in a ${fieldLength}-byte field`);
  }
  const buf = Buffer.alloc(fieldLength);
  encoded.copy(buf);
  return buf;
}

/**
 * Splits `path` into USTAR's `{prefix, name}` pair when it does not
 * fit in the 100-byte `name` field alone -- the standard POSIX ustar
 * extension (split at the LAST `/` such that both halves fit), not a
 * custom scheme. Throws explicitly (never silently truncates) if no
 * split makes it fit.
 */
function splitUstarPath(path: string): { prefix: string; name: string } {
  const bytes = Buffer.byteLength(path, "utf8");
  if (bytes <= NAME_MAX) return { prefix: "", name: path };
  const segments = path.split("/");
  for (let i = segments.length - 1; i > 0; i--) {
    const prefix = segments.slice(0, i).join("/");
    const name = segments.slice(i).join("/");
    if (Buffer.byteLength(prefix, "utf8") <= PREFIX_MAX && Buffer.byteLength(name, "utf8") <= NAME_MAX) {
      return { prefix, name };
    }
  }
  throw new Error(`tar-writer: path "${path}" (${bytes} bytes) does not fit USTAR's name+prefix fields even after splitting -- refusing to truncate or corrupt it`);
}

function buildHeader(entry: TarEntry): Buffer {
  const { prefix, name } = splitUstarPath(entry.path);
  const header = Buffer.alloc(BLOCK_SIZE);
  asciiField(name, NAME_MAX).copy(header, 0);
  octalField(entry.mode & 0o7777, 8).copy(header, 100);
  octalField(0, 8).copy(header, 108); // uid
  octalField(0, 8).copy(header, 116); // gid
  octalField(entry.content.length, 12).copy(header, 124); // size
  octalField(0, 12).copy(header, 136); // mtime -- epoch 0, deterministic, never host-clock-dependent
  header.fill(0x20, 148, 156); // checksum field: 8 ASCII spaces while computing
  header.write("0", 156, "ascii"); // typeflag '0' = regular file
  asciiField("", 100).copy(header, 157); // linkname -- unused
  asciiField("ustar", 6).copy(header, 257); // magic ("ustar\0", the 6th byte stays NUL from Buffer.alloc)
  asciiField("00", 2).copy(header, 263); // version
  asciiField("node", 32).copy(header, 265); // uname
  asciiField("node", 32).copy(header, 297); // gname
  octalField(0, 8).copy(header, 329); // devmajor
  octalField(0, 8).copy(header, 337); // devminor
  asciiField(prefix, PREFIX_MAX).copy(header, 345);

  let checksum = 0;
  for (let i = 0; i < BLOCK_SIZE; i++) checksum += header[i]!;
  const checksumField = Buffer.alloc(8);
  checksumField.write(checksum.toString(8).padStart(6, "0"), 0, "ascii");
  checksumField[6] = 0; // NUL
  checksumField[7] = 0x20; // space
  checksumField.copy(header, 148);

  return header;
}

function padToBlockSize(content: Buffer): Buffer {
  const remainder = content.length % BLOCK_SIZE;
  if (remainder === 0) return content;
  return Buffer.concat([content, Buffer.alloc(BLOCK_SIZE - remainder)]);
}

/** Builds a USTAR archive from `entries` (in the given order) and writes it to `outPath`. */
export async function writeTar(entries: readonly TarEntry[], outPath: string): Promise<void> {
  const parts: Buffer[] = [];
  for (const entry of entries) {
    parts.push(buildHeader(entry));
    parts.push(padToBlockSize(entry.content));
  }
  parts.push(Buffer.alloc(BLOCK_SIZE * 2)); // end-of-archive marker: two zero blocks
  await writeFile(outPath, Buffer.concat(parts));
}

function readOctalField(header: Buffer, offset: number, length: number): number {
  const text = header.toString("ascii", offset, offset + length).replace(/\0.*$/, "").trim();
  if (text.length === 0) return 0;
  const value = Number.parseInt(text, 8);
  if (!Number.isFinite(value)) {
    throw new Error(`tar-writer: could not parse octal field "${text}" at offset ${offset}`);
  }
  return value;
}

function readAsciiField(header: Buffer, offset: number, length: number): string {
  const nul = header.indexOf(0, offset);
  const end = nul >= 0 && nul < offset + length ? nul : offset + length;
  return header.toString("utf8", offset, end);
}

/**
 * Reads back a USTAR archive written by `writeTar` above -- the exact
 * counterpart, not a general-purpose tar reader (no extended/pax
 * headers, no GNU long-name extensions beyond the same prefix-
 * splitting `writeTar` itself produces, no non-regular-file types).
 * Used by `src/worker/docker-verify.ts`'s post-verification integrity
 * check to re-derive the EXPECTED `{path, mode, content}` set directly
 * from the same `candidate.tar` that was bind-mounted into the
 * container -- never from a second, independent re-materialization.
 */
export async function readTar(tarPath: string): Promise<TarEntry[]> {
  const buf = await readFile(tarPath);
  const entries: TarEntry[] = [];
  let offset = 0;
  while (offset + BLOCK_SIZE <= buf.length) {
    const header = buf.subarray(offset, offset + BLOCK_SIZE);
    if (header.every((b) => b === 0)) break; // end-of-archive marker
    const name = readAsciiField(header, 0, NAME_MAX);
    const mode = readOctalField(header, 100, 8);
    const size = readOctalField(header, 124, 12);
    const typeflag = String.fromCharCode(header[156]!);
    const prefix = readAsciiField(header, 345, PREFIX_MAX);
    if (typeflag !== "0" && typeflag !== "\0") {
      throw new Error(`tar-writer: readTar encountered an unsupported typeflag "${typeflag}" for "${name}" -- this reader only supports plain files written by writeTar`);
    }
    const path = prefix.length > 0 ? `${prefix}/${name}` : name;
    const contentStart = offset + BLOCK_SIZE;
    const content = Buffer.from(buf.subarray(contentStart, contentStart + size));
    entries.push({ path, mode, content });
    offset = contentStart + Math.ceil(size / BLOCK_SIZE) * BLOCK_SIZE;
  }
  return entries;
}
