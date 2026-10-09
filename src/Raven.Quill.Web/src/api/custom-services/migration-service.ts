import { z } from "zod";
import type { ApiClient } from "@/api/http-client";
import { streamNdjsonLines } from "@/api/custom-services/response-stream";
import type { CdcSinkTableConfig } from "@/api/generated/server-api";

// The interactive schema-migration planner streams one frame per thing that already happened: the
// agent's proposal, then a frame per collection as it is registered or rejected, then its reply.
// Everything except `type` is kept loose on purpose - this view exists to show what the agent did,
// so an unrecognised field should reach the log rather than fail the parse.
const migrationFrameSchema = z.discriminatedUnion("type", [
    z.object({
        type: z.literal("proposal"),
        areas: z
            .array(
                z.object({
                    area: z.string().nullish(),
                    collections: z.array(z.string()).default([]),
                    why: z.string().nullish(),
                }),
            )
            .default([]),
        collections: z
            .array(
                z.object({
                    collection: z.string().nullish(),
                    rootTable: z.string().nullish(),
                    absorbs: z
                        .array(
                            z.object({
                                table: z.string().nullish(),
                                how: z.string().nullish(),
                                why: z.string().nullish(),
                            }),
                        )
                        .default([]),
                    why: z.string().nullish(),
                }),
            )
            .default([]),
        dropped: z.array(z.object({ table: z.string().nullish(), why: z.string().nullish() })).default([]),
        enables: z.array(z.string()).default([]),
    }),
    z.object({
        type: z.literal("collection"),
        status: z.string(),
        collection: z.string(),
        version: z.number(),
        rationale: z.string().nullish(),
        config: z.unknown().optional(),
        warnings: z.array(z.string()).default([]),
    }),
    z.object({
        type: z.literal("rejected"),
        collection: z.string(),
        errors: z.array(z.string()).default([]),
    }),
    z.object({
        type: z.literal("removed"),
        collection: z.string().nullish(),
        reason: z.string().nullish(),
    }),
    z.object({
        type: z.literal("conventions"),
        propertyCase: z.string(),
        propertyLanguage: z.string().nullish(),
        notes: z.string().nullish(),
        mustReEmit: z.array(z.string()).default([]),
    }),
    z.object({ type: z.literal("note"), text: z.string() }),
    z.object({
        type: z.literal("reply"),
        reply: z.string().nullish(),
        gaps: z.array(z.string()).default([]),
        openQuestions: z
            .array(
                z.object({
                    question: z.string(),
                    options: z.array(z.string()).default([]),
                    recommended: z.number().nullish(),
                }),
            )
            .default([]),
    }),
    z.object({
        type: z.literal("done"),
        conversationId: z.string(),
    }),
    z.object({ type: z.literal("error"), message: z.string() }),
]);

export type MigrationFrame = z.infer<typeof migrationFrameSchema>;

export type MigrationStartRequest = {
    slug: string;
    selectedTables?: { sourceTableName: string; sourceTableSchema?: string | null }[];
    prompt?: string;
};

export type MigrationAskRequest = { slug: string; conversationId: string; prompt: string };

export type MigrationApplyRequest = { slug: string; conversationId: string; collections?: string[] };

export type MigrationRemoveCollectionRequest = { slug: string; conversationId: string; collection: string };

export type MigrationApplyResult = {
    configuration: { tables?: CdcSinkTableConfig[] | null } | null;
    unmappedTables: string[];
    errors: string[];
};

export function createMigrationService(client: ApiClient) {
    return {
        start: (request: MigrationStartRequest, signal?: AbortSignal) =>
            streamFrames(client, "/setup/migration/start", request, signal),

        ask: (request: MigrationAskRequest, signal?: AbortSignal) =>
            streamFrames(client, "/setup/migration/ask", request, signal),

        apply: (request: MigrationApplyRequest) => client.post<MigrationApplyResult>("/setup/migration/apply", request),

        removeCollection: (request: MigrationRemoveCollectionRequest) =>
            client.post<void>("/setup/migration/remove", request),
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
        const result = migrationFrameSchema.safeParse(JSON.parse(line) as unknown);

        if (!result.success) {
            throw new Error(`Received an unrecognised migration frame: ${line}`);
        }

        yield result.data;
    }
}
