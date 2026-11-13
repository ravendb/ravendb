import crypto from "node:crypto";
import os from "node:os";
import path from "node:path";
import { parseArgs } from "node:util";
// Playwright is not in package.json on purpose. It comes with `@storybook/test-runner`, so the screenshots
// and the story tests always run on the same Playwright version.
import { chromium } from "playwright";

const STORYBOOK_URL = "http://localhost:6006";
const RENDER_TIMEOUT_MS = 60_000;
// The webpack dev server keeps a hot-reload connection open, so the network never goes idle. A story is
// ready when its mocked data stopped changing the DOM. A story with a timer may never be quiet.
const QUIET_DOM_MS = 300;
const QUIET_DOM_TIMEOUT_MS = 5_000;
const MAX_HIDDEN_HEIGHT = 6_000;
const MAX_STORY_NAME_LENGTH = 40;
const THEMES = ["dark", "light", "blue", "classic"];
// Most users have a 1080p screen, and 720p is the smallest screen where every view must still look good.
const SCREENS = {
    "720p": { width: 1280, height: 720 },
    "1080p": { width: 1920, height: 1080 },
};
const DEFAULT_SCREEN = "1080p";
// A story renders only the view. In Studio the shell takes this space from the screen: the main menu (open
// by default), the navbar and the footer. The margin around the view is added by the `withBootstrap5` decorator.
const SHELL = { menuWidth: 280, navbarHeight: 70, footerHeight: 32 };
const USAGE = `usage: npm run storybook-screenshot -- <story-id> [--out file.png] [--theme ${THEMES.join("|")}] [--screen ${Object.keys(SCREENS).join("|")}] [--width px] [--height px] [--viewport-only] [--args name=value]...

  --screen           the screen of the user, ${DEFAULT_SCREEN} by default. The story gets the space that is left for the view
                     inside the Studio shell: the screen without the main menu, navbar and footer
  --width, --height  the exact size of the story viewport, instead of the size calculated from --screen
  --viewport-only    do not extend the screenshot to the content below the fold
  --args name=value  set a story arg before the story renders, like the Storybook controls do. Repeat it for more
                     args. The value is true or false, a number, one of the options, a date, any text, or JSON
                     for an array or object. Quote it when it has spaces or quotes`;

const { values: options, positionals } = parseArgs({
    allowPositionals: true,
    options: {
        out: { type: "string" },
        theme: { type: "string", default: "dark" },
        screen: { type: "string", default: DEFAULT_SCREEN },
        width: { type: "string" },
        height: { type: "string" },
        "viewport-only": { type: "boolean", default: false },
        args: { type: "string", multiple: true, default: [] },
    },
});

const [storyId] = positionals;
const isKnownScreen = Object.hasOwn(SCREENS, options.screen);
const viewSize = isKnownScreen ? getViewSizeInShell(SCREENS[options.screen]) : {};
const width = Number(options.width ?? viewSize.width);
const height = Number(options.height ?? viewSize.height);
const storyArgEntries = options.args.map((arg) => arg.match(/^([^=]+)=(.*)$/)?.slice(1));
const hasValidArguments =
    positionals.length === 1 &&
    isKnownScreen &&
    THEMES.includes(options.theme) &&
    isPositiveInteger(width) &&
    isPositiveInteger(height) &&
    storyArgEntries.every(Boolean);
if (!hasValidArguments) {
    console.error(USAGE);
    process.exit(1);
}
const storyArgs = Object.fromEntries(storyArgEntries.toSorted());
const hasStoryArgs = storyArgEntries.length > 0;

await ensureStorybookIsRunning();

const outPath = path.resolve(options.out ?? getDefaultOutPath());
const storyUrl = `${STORYBOOK_URL}/iframe.html?${new URLSearchParams({ id: storyId, viewMode: "story", globals: `theme:${options.theme}` })}`;

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
    page.on("pageerror", (error) => consoleErrors.push(error.message));

    await page.addInitScript(recordPlayFunctionError);
    if (hasStoryArgs) {
        await page.addInitScript(setStoryArgsBeforeRender, { storyId, requestedArgs: storyArgs });
    }
    await page.goto(storyUrl);
    // `sb-show-main` appears as soon as the story mounts, before its play function runs, so wait for the
    // whole render to finish instead. A story id that does not exist never starts a render.
    await page.waitForFunction(
        () =>
            window.__STORYBOOK_PREVIEW__?.currentRender?.phase === "finished" ||
            document.body.classList.contains("sb-show-errordisplay") ||
            document.body.classList.contains("sb-show-nopreview"),
        null,
        { timeout: RENDER_TIMEOUT_MS }
    );
    await waitForQuietDom(page);
    await page.evaluate(() => document.fonts.ready);

    if (!options["viewport-only"]) {
        await showContentBelowTheFold(page);
    }

    await page.screenshot({ path: outPath, fullPage: !options["viewport-only"] });
    console.log(`screenshot saved to ${outPath} (${describeSettings()})`);

    for (const error of consoleErrors) {
        console.error(`browser console error: ${error}`);
    }

    const failure =
        (await page.evaluate(getStoryFailure)) ?? (hasStoryArgs ? await page.evaluate(getArgsFailure) : null);
    if (failure) {
        console.error(`the story failed:\n${failure}`);
        process.exitCode = 1;
    }
} finally {
    await browser.close();
}

function getStoryFailure() {
    if (document.body.classList.contains("sb-show-errordisplay")) {
        return document.querySelector("#error-message")?.textContent ?? "unknown error";
    }
    if (document.body.classList.contains("sb-show-nopreview")) {
        return "no story with this id";
    }
    return window.__PLAY_FUNCTION_ERROR__ ?? null;
}

function getArgsFailure() {
    if (window.__STORY_ARGS_FAILURE__) {
        return window.__STORY_ARGS_FAILURE__;
    }
    return window.__STORY_ARGS_ARE_SET__ ? null : "the args were not set: Storybook did not load the story as expected";
}

// Storybook's own `args` URL parameter accepts only letters, digits, spaces, "-" and "_", and merges an array or
// object into the default value. So the args are set the way the Controls panel sets them, as soon as the story
// is loaded and before it renders.
function setStoryArgsBeforeRender({ storyId, requestedArgs }) {
    let preview;
    Object.defineProperty(window, "__STORYBOOK_PREVIEW__", {
        configurable: true,
        get: () => preview,
        set: (newPreview) => {
            preview = newPreview;
            let storyStore;
            Object.defineProperty(preview, "storyStoreValue", {
                configurable: true,
                get: () => storyStore,
                set: (newStoryStore) => {
                    storyStore = newStoryStore;
                    const setInitialArgs = storyStore.args.setInitial.bind(storyStore.args);
                    storyStore.args.setInitial = (story) => {
                        setInitialArgs(story);
                        if (story.id === storyId) {
                            setArgs(storyStore.args, story);
                        }
                    };
                },
            });
        },
    });

    function setArgs(argsStore, story) {
        const args = {};
        const errors = [];
        for (const [name, text] of Object.entries(requestedArgs)) {
            const argType = story.argTypes[name];
            const { value, error } = argType
                ? parseValue(text, argType, story.initialArgs[name])
                : { error: "the story has no such arg" };
            if (error) {
                errors.push(`${name}=${text}: ${error}`);
            } else {
                args[name] = value;
            }
        }
        if (errors.length > 0) {
            const storyArgs = Object.entries(story.argTypes).map(([name, argType]) => `${name}: ${describe(argType)}`);
            window.__STORY_ARGS_FAILURE__ = `wrong args:\n  ${errors.join("\n  ")}\nthe story has these args:\n  ${storyArgs.join("\n  ")}`;
            return;
        }
        argsStore.update(story.id, args);
        window.__STORY_ARGS_ARE_SET__ = true;
    }

    function parseValue(text, { options, type, control }, defaultValue) {
        if (options) {
            const optionIndex = options.findIndex((option) => String(option) === text);
            return optionIndex === -1
                ? { error: `must be one of ${options.join(", ")}` }
                : { value: options[optionIndex] };
        }
        // The date control keeps a timestamp, but a story can also use a Date or a date string.
        if (control?.type === "date") {
            const date = new Date(text);
            if (Number.isNaN(date.getTime())) {
                return { error: "must be a date, like 2026-11-13T10:00:00Z" };
            }
            if (defaultValue instanceof Date) {
                return { value: date };
            }
            return { value: typeof defaultValue === "string" ? text : date.getTime() };
        }
        switch (type?.name) {
            case "string":
                return { value: text };
            case "boolean":
                return ["true", "false"].includes(text)
                    ? { value: text === "true" }
                    : { error: "must be true or false" };
            case "number":
                return text.trim() !== "" && Number.isFinite(Number(text))
                    ? { value: Number(text) }
                    : { error: "must be a number" };
            case "array":
            case "object": {
                const value = parseJson(text);
                const isArray = Array.isArray(value);
                const isExpectedType =
                    type.name === "array" ? isArray : value !== null && typeof value === "object" && !isArray;
                return isExpectedType ? { value } : { error: `must be a JSON ${type.name}` };
            }
            case "function":
                return { error: "a function cannot be set from the command line" };
            default: {
                const value = parseJson(text);
                return { value: value === undefined ? text : value };
            }
        }
    }

    function parseJson(text) {
        try {
            return JSON.parse(text);
        } catch {
            return undefined;
        }
    }

    function describe({ options, type, control }) {
        if (options) {
            return options.join(" | ");
        }
        if (control?.type === "date") {
            return "date";
        }
        return ["array", "object"].includes(type?.name) ? `JSON ${type.name}` : (type?.name ?? "any value");
    }
}

// Story ids and args can be long. The hash keeps the name short, and different screenshots never share a file.
function getDefaultOutPath() {
    const storyName = storyId.split("--").at(-1).slice(0, MAX_STORY_NAME_LENGTH);
    const settings = [storyId, options.theme, width, height, options["viewport-only"], storyArgs];
    const settingsHash = crypto.createHash("sha256").update(JSON.stringify(settings)).digest("hex").slice(0, 8);
    return path.join(os.tmpdir(), "studio-story-screenshots", `${storyName}.${settingsHash}.png`);
}

function describeSettings() {
    const settings = [`theme ${options.theme}`, `story viewport ${width}x${height}`];
    const argsText = Object.entries(storyArgs)
        .map(([name, value]) => `${name}=${value}`)
        .join(", ");
    if (argsText) {
        settings.push(`args ${argsText}`);
    }
    return settings.join(", ");
}

// A failed play function leaves the story on screen, so Storybook's error display does not show it.
function recordPlayFunctionError() {
    let storybookChannel;
    Object.defineProperty(window, "__STORYBOOK_ADDONS_CHANNEL__", {
        configurable: true,
        get: () => storybookChannel,
        set: (channel) => {
            storybookChannel = channel;
            channel.on("playFunctionThrewException", (error) => {
                window.__PLAY_FUNCTION_ERROR__ = `the play function failed: ${error.message}`;
            });
        },
    });
}

function waitForQuietDom(page) {
    return page.evaluate(
        ([quietMs, timeoutMs]) =>
            new Promise((resolve) => {
                const finish = () => {
                    observer.disconnect();
                    resolve();
                };
                let quietTimer = setTimeout(finish, quietMs);
                const observer = new MutationObserver(() => {
                    clearTimeout(quietTimer);
                    quietTimer = setTimeout(finish, quietMs);
                });
                observer.observe(document.body, { subtree: true, childList: true, attributes: true });
                setTimeout(finish, timeoutMs);
            }),
        [QUIET_DOM_MS, QUIET_DOM_TIMEOUT_MS]
    );
}

// Studio pages scroll inside their own panels, not the document, so `fullPage` alone stops at the viewport.
async function showContentBelowTheFold(page) {
    const hiddenHeight = await page.evaluate(() => {
        const hiddenHeights = Array.from(document.querySelectorAll("body *"), (element) => {
            const isScrollable = ["auto", "scroll"].includes(getComputedStyle(element).overflowY);
            return isScrollable ? element.scrollHeight - element.clientHeight : 0;
        });
        return Math.max(0, ...hiddenHeights);
    });
    if (hiddenHeight > 0) {
        await page.setViewportSize({ width, height: height + Math.min(hiddenHeight, MAX_HIDDEN_HEIGHT) });
        await waitForQuietDom(page);
    }
}

function getViewSizeInShell(screen) {
    return {
        width: screen.width - SHELL.menuWidth,
        height: screen.height - SHELL.navbarHeight - SHELL.footerHeight,
    };
}

function isPositiveInteger(value) {
    return Number.isInteger(value) && value > 0;
}

async function ensureStorybookIsRunning() {
    try {
        await fetch(STORYBOOK_URL);
    } catch {
        console.error(`Storybook is not running at ${STORYBOOK_URL}; start it with \`npm run storybook\` first`);
        process.exit(1);
    }
}
