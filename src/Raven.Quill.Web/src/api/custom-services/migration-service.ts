import type { ApiClient } from "@/api/http-client";
import { streamNdjsonLines } from "@/api/custom-services/response-stream";
import {
    API_ENDPOINTS,
    type MigrationAskRequest,
    type MigrationFrame,
    type MigrationStartRequest,
} from "@/api/generated/server-api";

// The generated client hands back the raw response and cannot be cancelled. The planner streams one
// NDJSON frame per thing that already happened, which the view shows as it arrives, and the operator
// can stop a turn.
export function createMigrationService(client: ApiClient) {
    return {
        start: (request: MigrationStartRequest, signal?: AbortSignal) =>
            streamFrames(client, API_ENDPOINTS.setup.migrationStart, request, signal),

        ask: (request: MigrationAskRequest, signal?: AbortSignal) =>
            streamFrames(client, API_ENDPOINTS.setup.migrationAsk, request, signal),
    };
}

export type MigrationService = ReturnType<typeof createMigrationService>;

async function* streamFrames(
    client: ApiClient,
    path: string,
    body: unknown,
    signal?: AbortSignal,
): AsyncGenerator<MigrationFrame> {
    for await (const line of streamNdjsonLines(client, path, body, signal)) {
        const frame = parseFrame(line);

        if (frame) {
            yield frame;
        }
    }
}

// Like the server relay, an unreadable frame is skipped so the rest of the turn still arrives. A stream
// cut off mid-line then ends as a turn the planner did not finish.
function parseFrame(line: string): MigrationFrame | null {
    try {
        return JSON.parse(line) as MigrationFrame;
    } catch (error) {
        console.error("Skipped an unreadable planner frame:", line, error);
        return null;
    }
}
