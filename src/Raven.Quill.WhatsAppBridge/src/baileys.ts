import {
    makeWASocket,
    useMultiFileAuthState,
    fetchLatestBaileysVersion,
    fetchLatestWaWebVersion,
} from "@whiskeysockets/baileys";
import type { Logger } from "./logger.js";
import type { SocketFactory, WaSocket } from "./session.js";

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
            (socket as unknown as { ws: { on(event: string, cb: (node: unknown) => void): void } }).ws.on(
                "CB:iq,type:error",
                (node) => listener(describeIqError(node)),
            );
        };
        return waSocket;
    };
}
