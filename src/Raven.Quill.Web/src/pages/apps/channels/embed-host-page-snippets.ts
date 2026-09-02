/** The host half of the embed docs. The mint snippets show how to get a `url`; these show what a host
 *  actually does with it — per stack, each including the `expired` handshake that lets a session outlive
 *  its TTL. The mobile hosts wrap the url in a tiny local page because the widget only announces
 *  ready/expired/error to a page that frames it (see `packages/widget/src/host-channel.ts`). */

const IFRAME_MARKUP = [
    '<iframe id="quill" title="Assistant" style="width:100%;height:600px;border:0"></iframe>',
    "",
].join("\n");

/** Validate the envelope, then act on it. Written as the docs' one message-handling example so a host
 *  learns the origin check rather than trusting any parent-frame message. */
function envelopeHandlerLines(reopen: string) {
    return [
        "if (event.origin !== widgetOrigin) return;",
        "",
        "const message = event.data;",
        'if (message?.source !== "raven-quill" || message.version !== 1) return;',
        "",
        "switch (message.type) {",
        '    case "ready":',
        "        // the widget has mounted — hide your own loader here",
        "        break;",
        '    case "expired":',
        `        ${reopen}`,
        "        break;",
        '    case "error":',
        '        console.error("Quill widget error:", message.payload.message);',
        "        break;",
        "}",
    ];
}

function listenerLines(sessionOrigin: string, reopen: string) {
    return [
        `const widgetOrigin = "${sessionOrigin}";`,
        "",
        'window.addEventListener("message", (event) => {',
        ...indent(envelopeHandlerLines(reopen)),
        "});",
    ];
}

const OPEN_SESSION_COMMENT = [
    "// Your own endpoint. It calls the mint API server-side and returns just the `url`, so the",
    "// Dashboard API key never reaches the browser.",
];

const APPEARANCE_PROP_COMMENT =
    '// Pass "Light" or "Dark" to follow your app\'s own toggle, or "System" for the visitor\'s OS preference.';

const PIN_APPEARANCE_COMMENT =
    "// Optional: pin the color scheme at load with `${url}?appearance=dark` (or light/system)";

/** Your backend holds the Dashboard API key and exposes a thin mint endpoint of its own. */
export function buildHtmlHostSnippet(embedOrigin: string) {
    return [
        IFRAME_MARKUP,
        '<script type="module">',
        ...indent([
            ...OPEN_SESSION_COMMENT,
            "async function openSession() {",
            '    const response = await fetch("/api/quill-session", { method: "POST" });',
            "    if (!response.ok) throw new Error(`Could not start the assistant: ${response.status}`);",
            "",
            "    const { url } = await response.json();",
            `    ${PIN_APPEARANCE_COMMENT}`,
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

export function buildReactHostSnippet(embedOrigin: string) {
    return [
        'import { useEffect, useRef, useState } from "react";',
        "",
        `const widgetOrigin = "${embedOrigin}";`,
        "",
        'export function QuillAssistant({ appearance = "System" }) {',
        "    const frameRef = useRef(null);",
        "    const [url, setUrl] = useState(null);",
        "",
        ...indent(OPEN_SESSION_COMMENT),
        "    async function openSession() {",
        '        const response = await fetch("/api/quill-session", { method: "POST" });',
        "        if (!response.ok) throw new Error(`Could not start the assistant: ${response.status}`);",
        "",
        "        const { url: nextUrl } = await response.json();",
        `        ${PIN_APPEARANCE_COMMENT}`,
        "        setUrl(nextUrl);",
        "    }",
        "",
        "    useEffect(() => {",
        "        const onMessage = (event) => {",
        ...indent(envelopeHandlerLines("void openSession(); // mint a fresh link and keep chatting"), 3),
        "        };",
        "",
        '        window.addEventListener("message", onMessage);',
        "        void openSession();",
        '        return () => window.removeEventListener("message", onMessage);',
        "    }, []);",
        "",
        `    ${APPEARANCE_PROP_COMMENT}`,
        "    useEffect(() => {",
        "        frameRef.current.contentWindow.postMessage(",
        '            { source: "raven-quill", version: 1, type: "appearance", payload: { appearance } },',
        "            widgetOrigin,",
        "        );",
        "    }, [appearance]);",
        "",
        "    return (",
        '        <iframe ref={frameRef} src={url ?? undefined} title="Assistant" style={{ width: "100%", height: 600, border: 0 }} />',
        "    );",
        "}",
    ].join("\n");
}

export function buildVueHostSnippet(embedOrigin: string) {
    return [
        "<template>",
        '    <iframe ref="frame" :src="url" title="Assistant" style="width: 100%; height: 600px; border: 0"></iframe>',
        "</template>",
        "",
        "<script setup>",
        'import { onMounted, onUnmounted, ref, watch } from "vue";',
        "",
        `${APPEARANCE_PROP_COMMENT}`,
        'const props = defineProps({ appearance: { type: String, default: "System" } });',
        "",
        `const widgetOrigin = "${embedOrigin}";`,
        "const frame = ref(null);",
        "const url = ref(null);",
        "",
        ...OPEN_SESSION_COMMENT,
        "async function openSession() {",
        '    const response = await fetch("/api/quill-session", { method: "POST" });',
        "    if (!response.ok) throw new Error(`Could not start the assistant: ${response.status}`);",
        "",
        "    const { url: nextUrl } = await response.json();",
        `    ${PIN_APPEARANCE_COMMENT}`,
        "    url.value = nextUrl;",
        "}",
        "",
        "function onMessage(event) {",
        ...indent(envelopeHandlerLines("void openSession(); // mint a fresh link and keep chatting")),
        "}",
        "",
        "onMounted(() => {",
        '    window.addEventListener("message", onMessage);',
        "    void openSession();",
        "});",
        'onUnmounted(() => window.removeEventListener("message", onMessage));',
        "",
        "watch(",
        "    () => props.appearance,",
        "    (appearance) => {",
        "        frame.value.contentWindow.postMessage(",
        '            { source: "raven-quill", version: 1, type: "appearance", payload: { appearance } },',
        "            widgetOrigin,",
        "        );",
        "    },",
        ");",
        "</script>",
    ].join("\n");
}

/** The local page a mobile WebView wraps around the minted url. It relays the widget's envelope to the
 *  native side; `src` and `forward` carry each platform's own interpolation and bridge call. */
function webviewHostPageLines(embedOrigin: string, src: string, forward: string) {
    return [
        "<!doctype html>",
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        '<body style="margin:0">',
        `<iframe src="${src}" title="Assistant" style="width:100vw;height:100vh;border:0"></iframe>`,
        "<script>",
        '    window.addEventListener("message", (event) => {',
        `        if (event.origin !== "${embedOrigin}") return;`,
        "        const message = event.data;",
        '        if (message?.source !== "raven-quill" || message.version !== 1) return;',
        `        ${forward}`,
        "    });",
        "</script>",
    ];
}

// The wrapper page must load under a real origin: with allowed origins configured, the embed page's
// frame-ancestors CSP rejects the "null" origin a base-URL-less local page gets.
const MOBILE_BASE_URL_COMMENT = [
    "// The base url must be one of the channel's allowed origins (or leave the channel's origins",
    "// open) — the widget page refuses to be framed by a local page with no origin.",
];

const MOBILE_MINT_COMMENT = [
    "// Your own endpoint. It calls the mint API server-side and returns just the `url`, so the",
    "// Dashboard API key never ships inside the app.",
];

const MOBILE_PIN_APPEARANCE_COMMENT =
    "// Optional: pin the color scheme by appending ?appearance=dark (or light/system) to the url";

export function buildKotlinHostSnippet(embedOrigin: string) {
    return [
        "import android.util.Log",
        "import android.webkit.JavascriptInterface",
        "import android.webkit.WebView",
        "import kotlinx.coroutines.CoroutineScope",
        "import kotlinx.coroutines.Dispatchers",
        "import kotlinx.coroutines.launch",
        "",
        "class QuillAssistant(private val webView: WebView, private val scope: CoroutineScope) {",
        "",
        "    init {",
        "        webView.settings.javaScriptEnabled = true",
        "        webView.settings.domStorageEnabled = true",
        "        // The bridge is injected into every frame, the widget's included; the worst a caller gets is a fresh link.",
        '        webView.addJavascriptInterface(this, "QuillBridge")',
        "        openSession()",
        "    }",
        "",
        "    @JavascriptInterface",
        "    fun onQuillEvent(type: String, detail: String) {",
        "        when (type) {",
        '            "expired" -> openSession() // mint a fresh link and keep chatting',
        '            "error" -> Log.e("Quill", detail)',
        "        }",
        "    }",
        "",
        "    fun openSession() {",
        "        scope.launch(Dispatchers.IO) {",
        ...indent(MOBILE_MINT_COMMENT, 3),
        '            val url = fetchSessionUrl() // POST https://your.site/api/quill-session -> { "url": ... }',
        ...indent(MOBILE_BASE_URL_COMMENT, 3),
        "            //",
        `            ${MOBILE_PIN_APPEARANCE_COMMENT}`,
        "            webView.post {",
        '                webView.loadDataWithBaseURL("https://your.site", hostPage(url), "text/html", "utf-8", null)',
        "            }",
        "        }",
        "    }",
        "",
        '    private fun hostPage(url: String) = """',
        ...indent(
            webviewHostPageLines(
                embedOrigin,
                "$url",
                'QuillBridge.onQuillEvent(message.type, message.payload.message ?? "");',
            ),
            2,
        ),
        '    """.trimIndent()',
        "}",
    ].join("\n");
}

export function buildSwiftHostSnippet(embedOrigin: string) {
    return [
        "import WebKit",
        "",
        "final class QuillAssistant: NSObject, WKScriptMessageHandler {",
        "    // WKWebView.configuration is a copy, so the handler registers before the web view is created.",
        "    private(set) lazy var webView: WKWebView = {",
        "        let configuration = WKWebViewConfiguration()",
        "        // The handler is reachable from every frame, the widget's included; the worst a caller gets is a fresh link.",
        '        configuration.userContentController.add(self, name: "quill")',
        "        return WKWebView(frame: .zero, configuration: configuration)",
        "    }()",
        "",
        "    override init() {",
        "        super.init()",
        "        openSession()",
        "    }",
        "",
        "    func userContentController(_ controller: WKUserContentController, didReceive message: WKScriptMessage) {",
        "        guard let body = message.body as? [String: String] else { return }",
        '        switch body["type"] {',
        '        case "expired":',
        "            openSession() // mint a fresh link and keep chatting",
        '        case "error":',
        '            print("Quill widget error: \\(body["detail"] ?? "")")',
        "        default:",
        "            break",
        "        }",
        "    }",
        "",
        "    struct Mint: Decodable { let url: String }",
        "",
        "    func openSession() {",
        "        Task { @MainActor in",
        ...indent(MOBILE_MINT_COMMENT, 3),
        '            var request = URLRequest(url: URL(string: "https://your.site/api/quill-session")!)',
        '            request.httpMethod = "POST"',
        "            let (data, _) = try await URLSession.shared.data(for: request)",
        "",
        "            let url = try JSONDecoder().decode(Mint.self, from: data).url",
        ...indent(MOBILE_BASE_URL_COMMENT, 3),
        "            //",
        `            ${MOBILE_PIN_APPEARANCE_COMMENT}`,
        '            webView.loadHTMLString(hostPage(url: url), baseURL: URL(string: "https://your.site"))',
        "        }",
        "    }",
        "",
        "    private func hostPage(url: String) -> String {",
        '        """',
        ...indent(
            webviewHostPageLines(
                embedOrigin,
                "\\(url)",
                'window.webkit.messageHandlers.quill.postMessage({ type: message.type, detail: message.payload.message ?? "" });',
            ),
            2,
        ),
        '        """',
        "    }",
        "}",
    ].join("\n");
}

function indent(lines: string[], depth = 1) {
    const padding = "    ".repeat(depth);
    return lines.map((line) => (line.length === 0 ? line : `${padding}${line}`));
}
