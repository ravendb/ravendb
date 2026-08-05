import { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion } from "@whiskeysockets/baileys";
import type { Logger } from "./logger.js";
import type { SocketFactory, WaSocket } from "./session.js";

export function baileysSocketFactory(logger: Logger): SocketFactory {
    return async (authDir) => {
        const { state, saveCreds } = await useMultiFileAuthState(authDir);

        let version: [number, number, number] | undefined;
        try {
            ({ version } = await fetchLatestBaileysVersion());
        } catch {
            // offline: fall back to the version baked into the library
        }

        const socket = makeWASocket({
            auth: state,
            version,
            // Baileys accepts any pino-compatible logger; its own chatter stays at warn.
            logger: logger.child({ component: "baileys" }, { level: "warn" }) as never,
            markOnlineOnConnect: false,
            syncFullHistory: false,
            browser: ["Quill", "Chrome", "1.0"],
        });

        socket.ev.on("creds.update", saveCreds);
        return socket as unknown as WaSocket;
    };
}
