import type {
    IFrameCustomizationResponse,
    IFrameDefaultCustomizationResponse,
    IFramePreviewResponse,
    IFrameStyleGuideResponse,
} from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

// The widget's base stylesheet as the server ships it (see EmbedEndpoints.WidgetBaseCss): the
// generated :root variables followed by the layout rules, rule bodies on single lines. The editor
// formats this on load, so keeping it unformatted here mirrors the real payload.
export const SAMPLE_WIDGET_BASE_CSS = `:root {
  --ai-bg: #ffffff;
  --ai-fg: #0f172a;
  --ai-border-color: #e2e8f0;
  --ai-bubble-agent-bg: #f1f5f9;
  --ai-user-bg: #2563eb;
  --ai-user-fg: #ffffff;
  --ai-input-border-color: #cbd5e1;
  --ai-radius-bubble: 12px;
  --ai-radius-control: 8px;
  --ai-font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
}
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; background: var(--ai-bg); color: var(--ai-fg); font-family: var(--ai-font-family); }
  #ai-chat { display: flex; flex-direction: column; height: 100%; }
  #ai-chat-header { padding: 12px 16px; font-weight: 600; border-bottom: 1px solid var(--ai-border-color); }
  #ai-chat-feed { flex: 1; overflow-y: auto; padding: 12px 16px; }
  .row { margin: 6px 0; padding: 8px 12px; border-radius: var(--ai-radius-bubble); max-width: 80%; white-space: pre-wrap; }
  .row.user { background: var(--ai-user-bg); color: var(--ai-user-fg); margin-left: auto; }
  .row.agent { background: var(--ai-bubble-agent-bg); }
  #ai-chat-form { display: flex; gap: 8px; padding: 12px 16px; border-top: 1px solid var(--ai-border-color); }
  #ai-chat-input { flex: 1; padding: 10px 12px; border: 1px solid var(--ai-input-border-color); border-radius: var(--ai-radius-control); font-size: 14px; }
  #ai-chat-form button { padding: 10px 16px; border: 0; border-radius: var(--ai-radius-control); background: var(--ai-user-bg); color: var(--ai-user-fg); cursor: pointer; }`;

// A saved channel customization, formatted the way an operator leaves it after editing.
export const SAMPLE_CUSTOM_CSS = `:root {
    --ai-user-bg: #16a34a;
    --ai-radius-bubble: 4px;
}
#ai-chat-header {
    background: #16a34a;
    color: #ffffff;
}`;

// The app-level default a channel inherits when it has no CSS of its own.
export const SAMPLE_DEFAULT_CSS = `:root {
    --ai-user-bg: #7c3aed;
    --ai-font-family: Georgia, "Times New Roman", serif;
}`;

// The inert preview document the dashboard frames (see EmbedEndpoints.PreviewHtmlTemplate): base
// styles, an empty raven-custom slot the editor fills as the operator types, and sample bubbles.
const SAMPLE_PREVIEW_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Website widget</title>
<style>
${SAMPLE_WIDGET_BASE_CSS}
</style>
<style id="raven-custom"></style>
</head>
<body>
<div id="ai-chat">
  <div id="ai-chat-header">Website widget</div>
  <div id="ai-chat-feed">
    <div class="row agent">Hi! I'm your AI assistant. How can I help you today?</div>
    <div class="row user">What can you do?</div>
    <div class="row agent">I can answer questions about your data and help you get things done — just ask.</div>
  </div>
  <form id="ai-chat-form" onsubmit="return false">
    <input id="ai-chat-input" autocomplete="off" placeholder="Ask a question..." aria-label="Ask a question">
    <button type="submit">Send</button>
  </form>
</div>
</body>
</html>`;

export const iframeMocks = {
    getCustomization: (
        customization: IFrameCustomizationResponse = { css: SAMPLE_CUSTOM_CSS, defaultCss: SAMPLE_DEFAULT_CSS },
    ) =>
        apiHttp.get("/api/apps/{slug}/iframe/{widgetId}/customization", ({ response }) =>
            response(200).json(customization),
        ),
    updateCustomization: (defaultCss: string | null = SAMPLE_DEFAULT_CSS) =>
        apiHttp.put("/api/apps/{slug}/iframe/{widgetId}/customization", async ({ request, response }) => {
            const body = await request.json();
            return response(200).json({ css: body.css, defaultCss });
        }),
    getDefaultCustomization: (defaultCustomization: IFrameDefaultCustomizationResponse = { css: null }) =>
        apiHttp.get("/api/apps/{slug}/iframe/default-customization", ({ response }) =>
            response(200).json(defaultCustomization),
        ),
    updateDefaultCustomization: () =>
        apiHttp.put("/api/apps/{slug}/iframe/default-customization", async ({ request, response }) => {
            const body = await request.json();
            return response(200).json({ css: body.css });
        }),
    preview: (preview: IFramePreviewResponse = { html: SAMPLE_PREVIEW_HTML }) =>
        apiHttp.get("/api/apps/{slug}/iframe/preview", ({ response }) => response(200).json(preview)),
    styleGuide: (styleGuide: IFrameStyleGuideResponse = { baseCss: SAMPLE_WIDGET_BASE_CSS }) =>
        apiHttp.get("/api/apps/{slug}/iframe/style-guide", ({ response }) => response(200).json(styleGuide)),
};

// Happy-path handlers for every iframe endpoint (the story default). Because a story override
// replaces the whole `iframe` array, spread this after a single overriding handler to change one
// endpoint while keeping the rest — the override comes first, so it wins MSW's first-match.
export function iframeHandlers() {
    return [
        iframeMocks.getCustomization(),
        iframeMocks.updateCustomization(),
        iframeMocks.getDefaultCustomization(),
        iframeMocks.updateDefaultCustomization(),
        iframeMocks.preview(),
        iframeMocks.styleGuide(),
    ];
}
