import { describe, expect, it, vi } from "vitest";
import { createMigrationService } from "@/api/custom-services/migration-service";
import type { MigrationFrame } from "@/api/generated/server-api";
import { createApiClient } from "@/api/http-client";

function clientStreaming(body: string) {
    return createApiClient({
        transport: () => Promise.resolve(new Response(body, { headers: { "Content-Type": "application/x-ndjson" } })),
    });
}

async function collect(frames: AsyncGenerator<MigrationFrame>) {
    const collected: MigrationFrame[] = [];

    for await (const frame of frames) {
        collected.push(frame);
    }

    return collected;
}

describe("createMigrationService", () => {
    it("skips an unreadable frame and still yields the rest", async () => {
        const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
        const client = clientStreaming(
            '{"type":"note","text":"reading"}\n{"type":"no\n{"type":"done","conversationId":"c/1"}',
        );

        const frames = await collect(createMigrationService(client).start({ slug: "shop" }));

        expect(frames).toEqual([
            { type: "note", text: "reading" },
            { type: "done", conversationId: "c/1" },
        ]);
        expect(consoleError).toHaveBeenCalledOnce();
        consoleError.mockRestore();
    });
});
