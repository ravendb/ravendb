// Agents' embedded browsers often cannot register the MSW service worker, so any story that mocks the
// API fails to render there. Headless Chrome registers it fine.
import os from "node:os";
import path from "node:path";
import { parseArgs } from "node:util";
import { chromium } from "playwright";

const STORYBOOK_URL = "http://localhost:6106";
const RENDER_TIMEOUT_MS = 60_000;
// Some stories mock a request that never resolves, so the network may never go idle.
const NETWORK_IDLE_TIMEOUT_MS = 3_000;
const THEMES = ["light", "dark"];
const USAGE =
    "usage: pnpm storybook:screenshot <story-id> [--out file.png] [--theme light|dark] [--width 1440] [--height 900]";

const { values: options, positionals } = parseArgs({
    allowPositionals: true,
    options: {
        out: { type: "string" },
        theme: { type: "string", default: "light" },
        width: { type: "string", default: "1440" },
        height: { type: "string", default: "900" },
    },
});

const [storyId] = positionals;
const width = Number(options.width);
const height = Number(options.height);
const hasValidArguments =
    Boolean(storyId) && THEMES.includes(options.theme) && isPositiveInteger(width) && isPositiveInteger(height);
if (!hasValidArguments) {
    console.error(USAGE);
    process.exit(1);
}

await ensureStorybookIsRunning();

const outPath = path.resolve(options.out ?? path.join(os.tmpdir(), "quill-story-screenshots", `${storyId}.png`));
const storyUrl = `${STORYBOOK_URL}/iframe.html?id=${storyId}&viewMode=story&globals=theme:${options.theme}`;

const browser = await chromium.launch({ channel: "chrome" });
try {
    const page = await browser.newPage({ viewport: { width, height }, reducedMotion: "reduce" });
    const consoleErrors = [];
    page.on("console", (message) => {
        const sourceUrl = message.location().url;
        if (message.type() !== "error" || sourceUrl.endsWith("/favicon.ico")) {
            return;
        }
        consoleErrors.push(`${message.text()} (${sourceUrl})`);
    });

    await page.goto(storyUrl);
    // `sb-show-main` appears as soon as the story mounts, before its play function runs, so wait for the
    // whole render to finish instead. A story id that does not exist never starts a render, only the error display.
    await page.waitForFunction(
        () =>
            window.__STORYBOOK_PREVIEW__?.currentRender?.phase === "finished" ||
            document.body.classList.contains("sb-show-errordisplay"),
        null,
        { timeout: RENDER_TIMEOUT_MS },
    );
    await page.waitForLoadState("networkidle", { timeout: NETWORK_IDLE_TIMEOUT_MS }).catch(() => {});
    await page.evaluate(() => document.fonts.ready);

    // Page stories scroll inside AppShellMain, not the document, so `fullPage` alone stops at the viewport.
    const hiddenPageHeight = await page.evaluate(() => {
        const pageScrollArea = document.querySelector(".app-shell__main > :last-child");
        return pageScrollArea ? pageScrollArea.scrollHeight - pageScrollArea.clientHeight : 0;
    });
    if (hiddenPageHeight > 0) {
        await page.setViewportSize({ width, height: height + hiddenPageHeight });
    }

    await page.screenshot({ path: outPath, fullPage: true });
    console.log(`screenshot saved to ${outPath}`);

    for (const error of consoleErrors) {
        console.error(`browser console error: ${error}`);
    }

    const hasStoryFailed = await page.evaluate(() => document.body.classList.contains("sb-show-errordisplay"));
    if (hasStoryFailed) {
        console.error(`the story failed to render:\n${await page.locator("#error-message").innerText()}`);
        process.exitCode = 1;
    }
} finally {
    await browser.close();
}

function isPositiveInteger(value) {
    return Number.isInteger(value) && value > 0;
}

async function ensureStorybookIsRunning() {
    try {
        await fetch(STORYBOOK_URL);
    } catch {
        console.error(`Storybook is not running at ${STORYBOOK_URL}; start it with \`pnpm storybook\` first`);
        process.exit(1);
    }
}
