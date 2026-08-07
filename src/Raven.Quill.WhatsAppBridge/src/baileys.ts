import {
    makeWASocket,
    useMultiFileAuthState,
    fetchLatestBaileysVersion,
    fetchLatestWaWebVersion,
} from "@whiskeysockets/baileys";
import type { Logger } from "./logger.js";
import type { SocketFactory, WaSocket } from "./session.js";

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
        return socket as unknown as WaSocket;
    };
}
