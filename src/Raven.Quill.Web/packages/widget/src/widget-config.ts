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

export type WidgetMode = "live" | "preview";

export function readMode(search: string): WidgetMode {
    return new URLSearchParams(search).get("mode") === "preview" ? "preview" : "live";
}

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

export function readConfig(doc: Document): WidgetConfig {
    const element = doc.getElementById(CONFIG_ELEMENT_ID);
    if (element === null) throw new Error(`missing #${CONFIG_ELEMENT_ID} block`);
    return parseConfig(element.textContent ?? "");
}
