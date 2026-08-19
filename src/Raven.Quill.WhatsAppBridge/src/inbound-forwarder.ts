import type { Logger } from "./logger.js";
import { redactJid } from "./logger.js";

export interface InboundPayload {
    database: string;
    channelId: string;
    sender: string;
    messageId: string;
    kind: "text" | "unsupported";
    text: string | null;
    timestamp: number;
}

const DEFAULT_RETRY_DELAYS_MS = [1_000, 5_000, 15_000];

export class InboundForwarder {
    constructor(
        private readonly webInternalUrl: string,
        private readonly token: string,
        private readonly logger: Logger,
        private readonly retryDelaysMs: number[] = DEFAULT_RETRY_DELAYS_MS,
        private readonly fetchImpl: typeof fetch = fetch,
    ) {}

    async forward(payload: InboundPayload): Promise<void> {
        for (let attempt = 0; ; attempt++) {
            try {
                const response = await this.fetchImpl(`${this.webInternalUrl}/internal/whatsapp/inbound`, {
                    method: "POST",
                    headers: {
                        "content-type": "application/json",
                        "x-quill-bridge-token": this.token,
                    },
                    body: JSON.stringify(payload),
                });

                if (response.ok)
                    return;

                throw new Error(`web app responded ${response.status}`);
            } catch (error) {
                const delay = this.retryDelaysMs[attempt];
                if (delay === undefined) {
                    this.logger.warn(
                        { channelId: payload.channelId, sender: redactJid(payload.sender), error: String(error) },
                        "dropping inbound message after retries",
                    );
                    return;
                }

                await new Promise((resolve) => setTimeout(resolve, delay));
            }
        }
    }
}
