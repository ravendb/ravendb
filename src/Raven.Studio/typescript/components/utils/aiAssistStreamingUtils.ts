import { Path } from "react-hook-form";

interface ProcessStreamingResponseOptions<T extends object> {
    promiseFn: () => Promise<Response>;
    streamPropertyPath: Path<T>;
    onChunk?: (text: string) => void;
    onChunksCombined?: (text: string) => void;
    abortSignal?: AbortSignal;
}

type ProcessStreamingResult<T extends object> =
    | { status: Exclude<AiAssistantResponseStatus, "Success"> | "Error"; error: string }
    | { status: "Success"; data: T };

export async function processStreamingResponse<T extends object>({
    promiseFn,
    onChunk,
    onChunksCombined,
    streamPropertyPath,
    abortSignal,
}: ProcessStreamingResponseOptions<T>): Promise<ProcessStreamingResult<T>> {
    try {
        const response = await promiseFn();

        if (response.status === 500) {
            return {
                status: "Error",
                error: "Server is not responding. Please try again later.",
            };
        }

        const contentType = response.headers.get("content-type");

        if (!response.ok && contentType.includes("text/plain")) {
            const responseText = await response.text();
            return {
                status: "InternalError",
                error: responseText,
            };
        }

        if (contentType.includes("application/json")) {
            if (!response.ok) {
                try {
                    const data = await response.json();

                    // Get internal Status such as ConsentRequired, OutOfTokens, etc.
                    if (expectedErrorStatuses.includes(response.status) && data.Status) {
                        return { status: data.Status, error: data.Status };
                    }

                    return { status: "Error", error: data.Message };
                } catch (error) {
                    return { status: "Error", error: error instanceof Error ? error.message : "Unknown error" };
                }
            }

            return { status: "Success", data: await response.json() };
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder("utf-8");

        let streamText = "";

        while (true) {
            if (abortSignal?.aborted) {
                await reader.cancel();
                return { status: "Aborted", error: "Request was aborted" };
            }

            const { done, value } = await reader.read();
            if (done) {
                break;
            }

            const responseString = decoder.decode(value, { stream: true });
            const responseLines = responseString.split("\n");

            for (const line of responseLines) {
                if (!line.startsWith("data: ")) {
                    continue;
                }

                const dataString = line.trim().replace("data: ", "");
                const data: { text: string | T; type: "Ongoing" | "Done" | "Error" } = JSON.parse(dataString);

                if (data.type === "Error") {
                    return { status: "Error", error: "Unknown error" };
                }

                if (data.type === "Ongoing" && typeof data.text === "string") {
                    streamText += data.text;
                    onChunk?.(data.text);
                    onChunksCombined?.(streamText);
                }

                if (data.type === "Done" && typeof data.text === "object") {
                    const result = { ...data.text };
                    _.set(result, streamPropertyPath, streamText);
                    return { status: "Success", data: result };
                }
            }
        }

        return { status: "Error", error: "Failed to get the final response" };
    } catch (error) {
        if (error instanceof Error && error.name === "AbortError") {
            return { status: "Aborted", error: "Request was aborted" };
        }
        return { status: "Error", error: error instanceof Error ? error.message : "Unknown error" };
    }
}

const expectedErrorStatuses = [401, 429];
