// Folds the widget bundle into the dashboard's dist as dist/widget, so the single Docker COPY of dist into
// wwwroot lands it at wwwroot/widget - where WidgetAssets.cs looks for the Vite manifest.
//
// Must run after the dashboard's `vite build`, which empties dist.
import { cp, access } from "node:fs/promises";
import path from "node:path";

const ROOT = path.resolve(import.meta.dirname, "..");
const SOURCE = path.join(ROOT, "packages/widget/dist");
const TARGET = path.join(ROOT, "dist/widget");

try {
    await access(SOURCE);
} catch {
    console.error(`widget bundle not found at ${SOURCE}; run the widget package's build first`);
    process.exit(1);
}

await cp(SOURCE, TARGET, { recursive: true });
console.log(`copied the widget bundle to ${path.relative(ROOT, TARGET)}`);
