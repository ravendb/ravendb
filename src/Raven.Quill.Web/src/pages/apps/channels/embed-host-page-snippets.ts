/** The host-page half of the embed docs. The mint snippets show how to get a `url`; this shows what a
 *  page actually does with it, including the `expired` handshake that lets a session outlive its TTL. */

const IFRAME_MARKUP = [
    '<iframe id="quill" title="Assistant" style="width:100%;height:600px;border:0"></iframe>',
    "",
].join("\n");

/** Validate the envelope, then act on it. Written as the docs' one message-handling example so a host
 *  learns the origin check rather than trusting any parent-frame message. */
function listenerLines(sessionOrigin: string, reopen: string) {
    return [
        `const widgetOrigin = "${sessionOrigin}";`,
        "",
        'window.addEventListener("message", (event) => {',
        "    if (event.origin !== widgetOrigin) return;",
        "",
        "    const message = event.data;",
        '    if (message?.source !== "raven-quill" || message.version !== 1) return;',
        "",
        "    switch (message.type) {",
        '        case "ready":',
        "            // the widget has mounted — hide your own loader here",
        "            break;",
        '        case "expired":',
        `            ${reopen}`,
        "            break;",
        '        case "error":',
        '            console.error("Quill widget error:", message.payload.message);',
        "            break;",
        "    }",
        "});",
    ];
}

/** Your backend holds the Dashboard API key and exposes a thin mint endpoint of its own. */
export function buildBackedHostPageSnippet(embedOrigin: string) {
    return [
        IFRAME_MARKUP,
        '<script type="module">',
        ...indent([
            "// Your own endpoint. It calls the mint API server-side and returns just the `url`, so the",
            "// Dashboard API key never reaches the browser.",
            "async function openSession() {",
            '    const response = await fetch("/api/quill-session", { method: "POST" });',
            "    if (!response.ok) throw new Error(`Could not start the assistant: ${response.status}`);",
            "",
            "    const { url } = await response.json();",
            "    // Optional: pin the color scheme at load with `${url}?appearance=dark` (or light/system)",
            '    document.getElementById("quill").src = url;',
            "}",
            "",
            ...listenerLines(embedOrigin, "void openSession(); // mint a fresh link and keep chatting"),
            "",
            '// To follow your site\'s own light/dark toggle at runtime, send "Light" or "Dark" whenever it',
            '// changes, or "System" to follow the visitor\'s OS again:',
            "function setWidgetAppearance(appearance) {",
            '    document.getElementById("quill").contentWindow.postMessage(',
            '        { source: "raven-quill", version: 1, type: "appearance", payload: { appearance } },',
            "        widgetOrigin,",
            "    );",
            "}",
            "",
            "void openSession();",
        ]),
        "</script>",
    ].join("\n");
}

function indent(lines: string[]) {
    return lines.map((line) => (line.length === 0 ? line : `    ${line}`));
}
