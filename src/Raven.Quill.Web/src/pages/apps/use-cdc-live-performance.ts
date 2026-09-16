import { useEffect, useState } from "react";
import { MS_IN } from "@/lib/time";

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

export type CdcLiveBatchState = "success" | "pending" | "error";

export type CdcLiveBatch = {
    key: string;
    id: number;
    taskName: string;
    state: CdcLiveBatchState;
    started: string;
    ended: string | null;
    durationInMs: number;
    processed: number;
    scriptErrors: number;
    readErrors: number;
};

export type CdcLiveStatus = "active" | "error" | "idle";

export type CdcLivePerformance = {
    status: CdcLiveStatus;
    recentWrites: number;
    errorCount: number;
    batches: CdcLiveBatch[];
};

type TrackedBatch = {
    key: string;
    taskName: string;
    raw: CdcLiveRawBatch;
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
        const batches = new Map<string, TrackedBatch>();
        const socket = new WebSocket(buildProgressUrl(slug));
        let isDisposed = false;

        socket.onmessage = (event) => {
            if (isDisposed || typeof event.data !== "string") {
                return;
            }

            const frame = parseFrame(event.data);
            if (frame) {
                mergeFrame(batches, frame);
            }

            // Heartbeats (no frame) still land here every few seconds, keeping lag and
            // status fresh while the sink is idle.
            setState({
                connectionId,
                connection: "open",
                performance: shape(batches, Date.now()),
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
// task/process/batch identity instead of appending.
function mergeFrame(batches: Map<string, TrackedBatch>, frame: CdcLiveRawFrame) {
    for (const task of frame.Results ?? []) {
        const taskName = task.TaskName ?? "";
        (task.Stats ?? []).forEach((process, processIndex) => {
            for (const raw of process.Performance ?? []) {
                const key = `${taskName}/${processIndex}/${raw.Id}`;
                batches.set(key, { key, taskName, raw });
            }
        });
    }
}

// Client-side twin of the backend CdcPerformanceShaper, minus the inputs the live feed
// does not carry (task disabled flag, persisted last-activity timestamp).
const ACTIVE_WINDOW_MS = MS_IN.minute;

function shape(batches: Map<string, TrackedBatch>, nowMs: number): CdcLivePerformance {
    const orderedBatches = [...batches.values()]
        .map((tracked) => ({ ...tracked, startedMs: Date.parse(tracked.raw.Started) }))
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
        // A batch that moved nothing is noise in the log. An errored batch usually processed
        // nothing precisely because it failed, so that one still has to be visible.
        batches: orderedBatches
            .filter(({ raw }) => raw.NumberOfProcessedMessages > 0 || hasErrors(raw))
            .map(({ key, taskName, raw }) => ({
                key,
                id: raw.Id,
                taskName,
                state: toBatchState(raw),
                started: raw.Started,
                ended: raw.Completed ?? null,
                durationInMs: raw.DurationInMs,
                processed: raw.NumberOfProcessedMessages,
                scriptErrors: raw.ScriptProcessingErrorCount,
                readErrors: raw.ReadErrorCount,
            })),
    };
}

function hasErrors(raw: CdcLiveRawBatch): boolean {
    return raw.ScriptProcessingErrorCount + raw.ReadErrorCount > 0;
}

function toBatchState(raw: CdcLiveRawBatch): CdcLiveBatchState {
    if (hasErrors(raw)) {
        return "error";
    }

    return raw.Completed ? "success" : "pending";
}
