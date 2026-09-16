import type { ApiClient } from "@/api/http-client";

const DATA_FIELD_PREFIX = "data:";

/** POSTs `body` to `path` and yields the lines the server streams back, holding a partial trailing
 * line until the read that completes it. A trailing line the stream ended without a newline is still
 * yielded, flagged `isLineComplete: false`, so each framing can decide whether a cut-off tail counts.
 * Non-2xx responses (e.g. a pre-stream 400/401) throw an ApiError before the first line. Pass a
 * `signal` to cancel the request (callers abort it when the view unmounts mid-stream). */
export async function* streamResponseLines(
    client: ApiClient,
    path: string,
    body: unknown,
    signal?: AbortSignal,
): AsyncGenerator<{ line: string; isLineComplete: boolean }> {
    const response = await client.post<Response>(path, body, { responseType: "response", signal });

    if (!response.body) {
        return;
    }

    const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
    let buffer = "";

    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done) {
                break;
            }

            buffer += value;
            const lines = buffer.split("\n");
            buffer = lines.pop() ?? "";

            for (const line of lines) {
                yield { line, isLineComplete: true };
            }
        }

        if (buffer !== "") {
            yield { line: buffer, isLineComplete: false };
        }
    } finally {
        // Cancel the body so an aborted or abandoned generator stops the request streaming in the
        // background instead of only releasing the lock. cancel() also releases the reader; it
        // rejects on an already-errored stream (e.g. after an abort), which is safe to ignore.
        await reader.cancel().catch(() => {});
    }
}

/** The same stream framed as NDJSON: one JSON document per non-blank line. The last document keeps
 * counting without its trailing newline, which NDJSON writers routinely omit. */
export async function* streamNdjsonLines(
    client: ApiClient,
    path: string,
    body: unknown,
    signal?: AbortSignal,
): AsyncGenerator<string> {
    for await (const { line } of streamResponseLines(client, path, body, signal)) {
        if (line.trim()) {
            yield line;
        }
    }
}

/** The same stream framed as Server-Sent Events: the payload of every `data:` field. Comments, other
 * field names and the blank lines between events are skipped. So is an unterminated trailing line —
 * SSE frames always end in a newline, so that tail is a frame the connection cut off, and parsing it
 * would misreport the truncation as a malformed response (the SSE spec discards it too). */
export async function* streamSseData(
    client: ApiClient,
    path: string,
    body: unknown,
    signal?: AbortSignal,
): AsyncGenerator<string> {
    for await (const { line, isLineComplete } of streamResponseLines(client, path, body, signal)) {
        if (!isLineComplete) {
            continue;
        }

        const payload = readDataField(line);
        if (payload !== undefined) {
            yield payload;
        }
    }
}

function readDataField(line: string) {
    // trimEnd drops the trailing \r when the stream uses CRLF; SSE also allows one optional space
    // after the field's colon.
    const field = line.trimEnd();

    if (!field.startsWith(DATA_FIELD_PREFIX)) {
        return undefined;
    }

    const payload = field.slice(DATA_FIELD_PREFIX.length).trimStart();
    return payload === "" ? undefined : payload;
}
