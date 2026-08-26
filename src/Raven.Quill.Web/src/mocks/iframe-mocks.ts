import { delay } from "msw";
import type {
    ApiErrorResponse,
    WidgetDefaultThemeResponse,
    WidgetFontOption,
    WidgetTheme,
    WidgetThemeResponse,
} from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

// The curated stacks as the server ships them (see WidgetFonts.Curated). The editor renders its font select
// from this list rather than a copy of its own.
export const SAMPLE_FONT_OPTIONS: WidgetFontOption[] = [
    { label: "System", stack: 'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif' },
    { label: "Grotesque sans", stack: '"Helvetica Neue", Helvetica, Arial, sans-serif' },
    { label: "Geometric sans", stack: 'Verdana, "DejaVu Sans", Tahoma, sans-serif' },
    { label: "Serif", stack: 'Georgia, "Times New Roman", Times, serif' },
    { label: "Transitional serif", stack: 'Charter, "Iowan Old Style", Palatino, serif' },
    { label: "Monospace", stack: 'ui-monospace, "SF Mono", "Cascadia Mono", Menlo, Consolas, monospace' },
];

// The built-in default (see WidgetTheme.Default server-side).
export const SAMPLE_DEFAULT_THEME: WidgetTheme = {
    appearance: "System",
    light: { buttonColor: "#ff775f", messageColor: "#ffefec", backgroundColor: "#ffffff" },
    dark: { buttonColor: "#ff775f", messageColor: "#472928", backgroundColor: "#0d1117" },
    radius: "Medium",
    fontFamily: 'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif',
    fontSize: "Medium",
    customFontSizeRem: null,
    logo: null,
    logoRadius: "Pill",
    headerTitle: "AI Assistant",
    headerSubtitle: "Ask me anything",
    showHeader: true,
    greetingTitle: "How can I help?",
    greetingBody: "Ask a question and I'll do my best to answer it.",
    suggestedPrompts: [],
    inputPlaceholder: "Ask a question...",
    disclaimer: null,
    customCss: null,
};

// A saved channel theme, the way an operator leaves it after editing.
export const SAMPLE_CHANNEL_THEME: WidgetTheme = {
    appearance: "Light",
    light: { buttonColor: "#2f6f4f", messageColor: "#e0ece6", backgroundColor: "#f7f5f0" },
    dark: { buttonColor: "#4cc38a", messageColor: "#15302a", backgroundColor: "#0d1117" },
    radius: "Large",
    fontFamily: 'Georgia, "Times New Roman", Times, serif',
    fontSize: "Medium",
    customFontSizeRem: null,
    logo: null,
    logoRadius: "Pill",
    headerTitle: "Order support",
    headerSubtitle: "We usually reply instantly",
    showHeader: true,
    greetingTitle: "Need a hand with an order?",
    greetingBody: "Ask about delivery, returns or anything else.",
    suggestedPrompts: ["Where is my order?", "How do I return an item?", "Do you ship internationally?"],
    inputPlaceholder: "Type a message...",
    disclaimer: "Answers are AI generated and may be inaccurate.",
    customCss: null,
};

export const iframeMocks = {
    getTheme: (
        theme: WidgetThemeResponse = {
            theme: SAMPLE_CHANNEL_THEME,
            defaultTheme: SAMPLE_DEFAULT_THEME,
            fontOptions: SAMPLE_FONT_OPTIONS,
        },
    ) => apiHttp.get("/api/apps/{slug}/iframe/{channelId}/theme", ({ response }) => response(200).json(theme)),
    /** Never answers, so the theme page stays in its loading state. */
    getThemePending: () =>
        apiHttp.get("/api/apps/{slug}/iframe/{channelId}/theme", async ({ response }) => {
            await delay("infinite");
            return response(200).json({
                theme: SAMPLE_CHANNEL_THEME,
                defaultTheme: SAMPLE_DEFAULT_THEME,
                fontOptions: SAMPLE_FONT_OPTIONS,
            });
        }),
    getThemeError: (error: ApiErrorResponse = { error: "Could not load the theme.", code: "theme_load_failed" }) =>
        apiHttp.get("/api/apps/{slug}/iframe/{channelId}/theme", ({ response }) => response(404).json(error)),
    updateTheme: (defaultTheme: WidgetTheme = SAMPLE_DEFAULT_THEME) =>
        apiHttp.put("/api/apps/{slug}/iframe/{channelId}/theme", async ({ request, response }) => {
            const body = await request.json();
            return response(200).json({ theme: body.theme, defaultTheme, fontOptions: SAMPLE_FONT_OPTIONS });
        }),
    updateThemeError: (error: ApiErrorResponse = { error: "Could not save the theme.", code: "theme_save_failed" }) =>
        apiHttp.put("/api/apps/{slug}/iframe/{channelId}/theme", ({ response }) => response(400).json(error)),
    getDefaultTheme: (
        defaultTheme: WidgetDefaultThemeResponse = {
            theme: SAMPLE_DEFAULT_THEME,
            fontOptions: SAMPLE_FONT_OPTIONS,
        },
    ) => apiHttp.get("/api/apps/{slug}/iframe/default-theme", ({ response }) => response(200).json(defaultTheme)),
    getDefaultThemeError: (
        error: ApiErrorResponse = { error: "Could not load the default theme.", code: "default_theme_load_failed" },
    ) => apiHttp.get("/api/apps/{slug}/iframe/default-theme", ({ response }) => response(404).json(error)),
    updateDefaultTheme: () =>
        apiHttp.put("/api/apps/{slug}/iframe/default-theme", async ({ request, response }) => {
            const body = await request.json();
            return response(200).json({
                theme: body.theme ?? SAMPLE_DEFAULT_THEME,
                fontOptions: SAMPLE_FONT_OPTIONS,
            });
        }),
};

// A stateful pair for the channel theme GET/PUT. Every other mock here always answers with its fixed
// fixture, so a refetch after a save never observes what was actually saved. This one echoes the last
// PUT body back from the GET, like the real endpoint, so a save-then-reseed flow can be exercised.
export function statefulThemeMocks(
    initialTheme: WidgetTheme | null = SAMPLE_CHANNEL_THEME,
    defaultTheme: WidgetTheme = SAMPLE_DEFAULT_THEME,
) {
    let currentTheme = initialTheme;

    return {
        getTheme: () =>
            apiHttp.get("/api/apps/{slug}/iframe/{channelId}/theme", ({ response }) =>
                response(200).json({ theme: currentTheme, defaultTheme, fontOptions: SAMPLE_FONT_OPTIONS }),
            ),
        updateTheme: () =>
            apiHttp.put("/api/apps/{slug}/iframe/{channelId}/theme", async ({ request, response }) => {
                const body = await request.json();
                currentTheme = body.theme;
                return response(200).json({ theme: currentTheme, defaultTheme, fontOptions: SAMPLE_FONT_OPTIONS });
            }),
    };
}

// Happy-path handlers for every iframe endpoint (the story default). Because a story override replaces the
// whole `iframe` array, spread this after a single overriding handler to change one endpoint while keeping
// the rest — the override comes first, so it wins MSW's first-match.
export function iframeHandlers() {
    return [
        iframeMocks.getTheme(),
        iframeMocks.updateTheme(),
        iframeMocks.getDefaultTheme(),
        iframeMocks.updateDefaultTheme(),
    ];
}
