import { MS_IN } from "@/lib/time";

const CONNECTING_POLL_MS = 3 * MS_IN.second;
const IDLE_POLL_MS = 30 * MS_IN.second;
const CONNECTING_POLL_WINDOW_MS = MS_IN.minute;

export type ConnectingRow = {
    channelId: string;
    enabled: boolean;
    connected: boolean;
    error: string | null;
};

export function createConnectingPoll<TRow>(baseKey: string, project: (row: TRow) => ConnectingRow) {
    const connectingSinceByChannel = new Map<string, number>();

    return (slug: string, rows: TRow[]) => {
        const now = Date.now();
        const startedAt: number[] = [];

        for (const raw of rows) {
            const row = project(raw);
            const key = `${baseKey}/${slug}/${row.channelId}`;
            const isConnecting = row.enabled && !row.connected && !row.error;

            if (isConnecting === false) {
                connectingSinceByChannel.delete(key);
                continue;
            }

            const since = connectingSinceByChannel.get(key) ?? now;
            connectingSinceByChannel.set(key, since);
            startedAt.push(since);
        }

        return startedAt.some((since) => now - since < CONNECTING_POLL_WINDOW_MS) ? CONNECTING_POLL_MS : IDLE_POLL_MS;
    };
}
