import { DEFAULT_THEME, type WidgetTheme } from "@/widget-theme";

export type ChatRole = "user" | "assistant";

export type HistoryTurn = { role: ChatRole; content: string };

/** The live payload the server's shell embeds as `<script type="application/json" id="rq-config">`. */
export type WidgetConfig = {
    mode: "live";
    chatUrl: string;
    theme: WidgetTheme;
    history: HistoryTurn[];
};

export const CONFIG_ELEMENT_ID = "rq-config";

/** Server-supplied themes may predate a field the widget already reads, so every load merges over the
 *  defaults rather than trusting the payload to be complete. */
export function normalizeTheme(theme: Partial<WidgetTheme> | null | undefined): WidgetTheme {
    return { ...DEFAULT_THEME, ...(theme ?? {}) };
}

function isHistoryTurn(value: unknown): value is HistoryTurn {
    if (typeof value !== "object" || value === null) return false;
    const turn = value as Record<string, unknown>;
    return (turn.role === "user" || turn.role === "assistant") && typeof turn.content === "string";
}

export function parseConfig(json: string): WidgetConfig {
    const raw = JSON.parse(json) as Partial<WidgetConfig>;
    if (typeof raw.chatUrl !== "string" || raw.chatUrl.length === 0)
        throw new Error("widget config is missing chatUrl");

    return {
        mode: "live",
        chatUrl: raw.chatUrl,
        theme: normalizeTheme(raw.theme),
        history: Array.isArray(raw.history) ? raw.history.filter(isHistoryTurn) : [],
    };
}

/** Null when the document carries no config block at all - the dev harness and the dashboard's preview
 *  frame, as opposed to a shell the server rendered for a live link. */
export function readConfigJson(doc: Document): string | null {
    return doc.getElementById(CONFIG_ELEMENT_ID)?.textContent ?? null;
}

/** `unusable` means the shell was served wrong: it claims to be live but its config block is absent or
 *  malformed, and there is nothing to render. */
export type WidgetMount = { mode: "live"; config: WidgetConfig } | { mode: "preview" } | { mode: "unusable" };

/** The config block, not the URL, is what makes a document live. A live shell passes its own query string
 *  down to this document — `?appearance=` is a documented host option — so honouring `?mode=preview` there
 *  would swap a visitor's real conversation for the canned demo transcript. */
export function resolveMount(configJson: string | null, search: string): WidgetMount {
    if (configJson === null)
        return new URLSearchParams(search).get("mode") === "preview" ? { mode: "preview" } : { mode: "unusable" };

    try {
        return { mode: "live", config: parseConfig(configJson) };
    } catch {
        return { mode: "unusable" };
    }
}
