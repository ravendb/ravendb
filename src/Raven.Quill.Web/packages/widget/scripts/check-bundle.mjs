// Guards the widget's initial payload budget. The widget ships to third-party websites, so a
// dependency bump has to be reviewed on a measured number rather than on hope.
//
// Counts the entry chunk, everything it statically imports (transitively), and its CSS - i.e. exactly
// what a browser must download before the widget can paint. Lazy chunks are excluded by design.
import { gzipSync } from "node:zlib";
import { readFile } from "node:fs/promises";
import path from "node:path";

const BUDGET_BYTES = 200 * 1024;
const DIST_DIR = path.resolve(import.meta.dirname, "../dist");
const MANIFEST_PATH = path.join(DIST_DIR, ".vite/manifest.json");

function formatKb(bytes) {
    return `${(bytes / 1024).toFixed(1)} KB`;
}

async function gzippedSize(file) {
    const contents = await readFile(path.join(DIST_DIR, file));
    return gzipSync(contents, { level: 9 }).byteLength;
}

function collectInitialFiles(manifest) {
    const entry = Object.values(manifest).find((chunk) => chunk.isEntry);
    if (entry === undefined) throw new Error("no entry chunk in the manifest");

    const files = new Set();
    const visit = (chunk) => {
        if (chunk === undefined || files.has(chunk.file)) return;
        files.add(chunk.file);
        for (const css of chunk.css ?? []) files.add(css);
        for (const key of chunk.imports ?? []) visit(manifest[key]);
    };

    visit(entry);
    return [...files];
}

const manifest = JSON.parse(await readFile(MANIFEST_PATH, "utf8"));
const files = collectInitialFiles(manifest);

const sizes = await Promise.all(files.map(async (file) => ({ file, bytes: await gzippedSize(file) })));
sizes.sort((a, b) => b.bytes - a.bytes);

const total = sizes.reduce((sum, { bytes }) => sum + bytes, 0);

console.log("widget initial payload (gzip -9):");
for (const { file, bytes } of sizes) console.log(`  ${formatKb(bytes).padStart(9)}  ${file}`);
console.log(`  ${"-".repeat(9)}`);
console.log(`  ${formatKb(total).padStart(9)}  total  (budget ${formatKb(BUDGET_BYTES)})`);

if (total > BUDGET_BYTES) {
    console.error(`\nover budget by ${formatKb(total - BUDGET_BYTES)}`);
    process.exit(1);
}

console.log(`\n${formatKb(BUDGET_BYTES - total)} of headroom`);
