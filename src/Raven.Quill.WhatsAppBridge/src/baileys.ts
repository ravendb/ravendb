import {
    makeWASocket,
    useMultiFileAuthState,
    fetchLatestBaileysVersion,
    fetchLatestWaWebVersion,
} from "@whiskeysockets/baileys";
import type { Logger } from "./logger.js";
import type { SocketFactory, WaSocket } from "./session.js";

/// <iq type="error"><error code="400" text="bad-request"/></iq> -> "400 bad-request"
function describeIqError(node: unknown): string {
    const content = (node as { content?: unknown }).content;
    const error = Array.isArray(content)
        ? (content.find((child) => (child as { tag?: string }).tag === "error") as
              | { attrs?: { code?: string; text?: string } }
              | undefined)
        : undefined;

    const code = error?.attrs?.code;
    const text = error?.attrs?.text;
    return [code, text].filter(Boolean).join(" ") || "unknown error";
}

export function baileysSocketFactory(logger: Logger): SocketFactory {
    return async (authDir) => {
        const { state, saveCreds } = await useMultiFileAuthState(authDir);

        // fetchLatestBaileysVersion reports isLatest:true while returning a stale WA Web
        // version, which lets pairing start but makes WhatsApp refuse to finish the link
        // (both QR and pairing code). wa-web is the authoritative source; the Baileys
        // endpoint stays as a fallback. Upstream: WhiskeySockets/Baileys#2679.
        let version: [number, number, number] | undefined;
        for (const fetchVersion of [fetchLatestWaWebVersion, fetchLatestBaileysVersion]) {
            try {
                ({ version } = await fetchVersion());
                break;
            } catch {
                // try the next source; offline falls through to the baked-in version
            }
        }

        logger.info({ version }, "whatsapp client version");

        // Baileys chatter stays at warn unless the bridge itself runs at debug/trace.
        const baileysLevel = logger.level === "debug" || logger.level === "trace" ? logger.level : "warn";
        const socket = makeWASocket({
            auth: state,
            version,
            logger: logger.child({ component: "baileys" }, { level: baileysLevel }) as never,
            markOnlineOnConnect: false,
            syncFullHistory: false,
            browser: ["Quill", "Chrome", "1.0"],
        });

        socket.ev.on("creds.update", saveCreds);

        const waSocket = socket as unknown as WaSocket;
        waSocket.onIqError = (listener) => {
            // Baileys fans every received node out over CB:<tag>,<attr>:<value>; an iq
            // error with no pending query would otherwise only reach the debug log.
            (socket as unknown as { ws: { on(event: string, cb: (node: unknown) => void): void } }).ws.on(
                "CB:iq,type:error",
                (node) => listener(describeIqError(node)),
            );
        };
        return waSocket;
    };
}
