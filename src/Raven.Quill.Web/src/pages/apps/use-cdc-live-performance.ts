import { useEffect, useState } from "react";

// Frame shape of RavenDB's cdc-sink live stats feed, relayed verbatim by the WS-only
// /api/apps/{slug}/cdc/progress route, so it is not part of the generated OpenAPI client.
export type CdcLiveRawBatch = {
    Id: number;
    Started: string;
    Completed?: string | null;
    DurationInMs: number;
    NumberOfReadMessages: number;
    NumberOfProcessedMessages: number;
    ScriptProcessingErrorCount: number;
    ReadErrorCount: number;
};

export type CdcLiveRawFrame = {
    Results?: {
        TaskName?: string;
        Stats?: { Performance?: CdcLiveRawBatch[] }[];
    }[];
};

export type CdcLiveBatch = {
    key: string;
    started: string;
    ended: string | null;
    durationInMs: number;
    processed: number;
    errors: number;
};

export type CdcLiveStatus = "active" | "error" | "idle";

export type CdcLivePerformance = {
    status: CdcLiveStatus;
    recentWrites: number;
    errorCount: number;
    // Batches observed since the connection opened, including ones pruned from recentBatches.
    totalBatches: number;
    recentBatches: CdcLiveBatch[];
};

type CdcLiveConnection = "connecting" | "error" | "open";

type CdcLiveState = {
    connectionId: string;
    connection: CdcLiveConnection;
    performance: CdcLivePerformance | null;
};

export function useCdcLivePerformance(slug: string) {
    const [attempt, setAttempt] = useState(0);
    const connectionId = `${slug}#${attempt}`;
    const [state, setState] = useState<CdcLiveState>(() => ({
        connectionId,
        connection: "connecting",
        performance: null,
    }));

    // Reset during render (not in the effect) when the slug changes or a retry starts.
    if (state.connectionId !== connectionId) {
        setState({ connectionId, connection: "connecting", performance: null });
    }

    useEffect(() => {
        const batches = new Map<string, CdcLiveRawBatch>();
        const socket = new WebSocket(buildProgressUrl(slug));
        let isDisposed = false;
        let totalBatches = 0;

        socket.onmessage = (event) => {
            if (isDisposed || typeof event.data !== "string") {
                return;
            }

            const frame = parseFrame(event.data);
            if (frame) {
                totalBatches += mergeFrame(batches, frame);
            }

            // Heartbeats (no frame) still land here every few seconds, keeping lag and
            // status fresh while the sink is idle.
            setState({
                connectionId,
                connection: "open",
                performance: shape(batches, totalBatches, Date.now()),
            });
        };

        const fail = () => {
            if (!isDisposed) {
                setState({ connectionId, connection: "error", performance: null });
            }
        };
        socket.onerror = fail;
        socket.onclose = fail;

        return () => {
            isDisposed = true;
            socket.close();
        };
    }, [slug, connectionId]);

    return {
        connection: state.connection,
        performance: state.performance,
        retry: () => setAttempt((value) => value + 1),
    };
}

function buildProgressUrl(slug: string) {
    const protocol = window.location.protocol === "https:" ? "wss" : "ws";
    return `${protocol}://${window.location.host}/api/apps/${encodeURIComponent(slug)}/cdc/progress`;
}

function parseFrame(data: string): CdcLiveRawFrame | null {
    const text = data.trim();
    // An all-whitespace message is the feed's heartbeat.
    if (!text) {
        return null;
    }

    try {
        return JSON.parse(text) as CdcLiveRawFrame;
    } catch {
        return null;
    }
}

// The feed resends an in-progress batch (same Id) until it completes, so merge by
// task/process/batch identity instead of appending. Returns the number of batches not
// seen before, so the caller can keep a running total across pruning.
function mergeFrame(batches: Map<string, CdcLiveRawBatch>, frame: CdcLiveRawFrame): number {
    let newBatches = 0;
    for (const task of frame.Results ?? []) {
        (task.Stats ?? []).forEach((process, processIndex) => {
            for (const batch of process.Performance ?? []) {
                const key = `${task.TaskName ?? ""}/${processIndex}/${batch.Id}`;
                if (!batches.has(key)) {
                    newBatches += 1;
                }
                batches.set(key, batch);
            }
        });
    }

    pruneOldest(batches);
    return newBatches;
}

export const MAX_TRACKED_BATCHES = 500;

function pruneOldest(batches: Map<string, CdcLiveRawBatch>) {
    if (batches.size <= MAX_TRACKED_BATCHES) {
        return;
    }

    const oldestFirst = [...batches.entries()].sort(([, a], [, b]) => Date.parse(a.Started) - Date.parse(b.Started));
    for (const [key] of oldestFirst.slice(0, batches.size - MAX_TRACKED_BATCHES)) {
        batches.delete(key);
    }
}

// Client-side twin of the backend CdcPerformanceShaper, minus the inputs the live feed
// does not carry (task disabled flag, persisted last-activity timestamp).
const ACTIVE_WINDOW_MS = 60_000;

function shape(batches: Map<string, CdcLiveRawBatch>, totalBatches: number, nowMs: number): CdcLivePerformance {
    const orderedBatches = [...batches.entries()]
        .map(([key, raw]) => ({ key, raw, startedMs: Date.parse(raw.Started) }))
        .sort((a, b) => a.startedMs - b.startedMs);

    let recentWrites = 0;
    let errorCount = 0;
    let lastSyncMs: number | null = null;
    let hasBatchInProgress = false;

    for (const { raw } of orderedBatches) {
        recentWrites += raw.NumberOfProcessedMessages;
        errorCount += raw.ScriptProcessingErrorCount + raw.ReadErrorCount;

        if (raw.Completed) {
            const completedMs = Date.parse(raw.Completed);
            if (lastSyncMs === null || completedMs > lastSyncMs) {
                lastSyncMs = completedMs;
            }
        } else {
            hasBatchInProgress = true;
        }
    }

    const isRecentlyActive = lastSyncMs !== null && nowMs - lastSyncMs <= ACTIVE_WINDOW_MS;
    const status: CdcLiveStatus = errorCount > 0 ? "error" : hasBatchInProgress || isRecentlyActive ? "active" : "idle";

    return {
        status,
        recentWrites,
        errorCount,
        totalBatches,
        recentBatches: orderedBatches.map(({ key, raw }) => ({
            key,
            started: raw.Started,
            ended: raw.Completed ?? null,
            durationInMs: raw.DurationInMs,
            processed: raw.NumberOfProcessedMessages,
            errors: raw.ScriptProcessingErrorCount + raw.ReadErrorCount,
        })),
    };
}
