import type { ApiClient } from "@/api/http-client";
import { streamNdjsonLines } from "@/api/custom-services/response-stream";
import {
    API_ENDPOINTS,
    type MigrationAreaAskRequest,
    type MigrationAreaStartRequest,
    type MigrationFrame,
    type MigrationProposalAskRequest,
    type MigrationProposalStartRequest,
} from "@/api/generated/server-api";

// The generated client hands back the raw response and cannot be cancelled. Every planner turn streams
// one NDJSON frame per thing that already happened, which the view shows as it arrives. Aborting only
// disconnects the browser: the turn goes on on the server until migrationStop ends it.
export function createMigrationService(client: ApiClient) {
    return {
        proposalStart: (request: MigrationProposalStartRequest, signal?: AbortSignal) =>
            streamFrames(client, API_ENDPOINTS.setup.migrationProposalStart, request, signal),

        proposalAsk: (request: MigrationProposalAskRequest, signal?: AbortSignal) =>
            streamFrames(client, API_ENDPOINTS.setup.migrationProposalAsk, request, signal),

        areaStart: (request: MigrationAreaStartRequest, signal?: AbortSignal) =>
            streamFrames(client, API_ENDPOINTS.setup.migrationAreaStart, request, signal),

        areaAsk: (request: MigrationAreaAskRequest, signal?: AbortSignal) =>
            streamFrames(client, API_ENDPOINTS.setup.migrationAreaAsk, request, signal),
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
