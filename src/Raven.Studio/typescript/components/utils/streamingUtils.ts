import { Path } from "react-hook-form";

interface ProcessStreamingResponseOptions<T extends object> {
    promiseFn: () => Promise<Response>;
    streamPropertyPath: Path<T>;
    onChunk?: (text: string) => void;
    onChunksCombined?: (text: string) => void;
}

type ProcessStreamingResult<T extends object> = { status: "error"; error: string } | { status: "success"; data: T };

export async function processStreamingResponse<T extends object>({
    promiseFn,
    onChunk,
    onChunksCombined,
    streamPropertyPath,
}: ProcessStreamingResponseOptions<T>): Promise<ProcessStreamingResult<T>> {
    try {
        const response = await promiseFn();

        if (response.headers.get("content-type").includes("application/json")) {
            if (!response.ok) {
                try {
                    const data = await response.json();
                    return { status: "error", error: data.Message };
                } catch (error) {
                    return { status: "error", error: error instanceof Error ? error.message : "Unknown error" };
                }
            }

            return { status: "success", data: await response.json() };
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder("utf-8");

        let streamText = "";

        while (true) {
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
                const data: { text: string | T; done: boolean } = JSON.parse(dataString);

                if (!data.done && typeof data.text === "string") {
                    streamText += data.text;
                    onChunk?.(data.text);
                    onChunksCombined?.(streamText);
                }

                if (data.done && typeof data.text === "object") {
                    const result = { ...data.text };
                    _.set(result, streamPropertyPath, streamText);
                    return { status: "success", data: result };
                }
            }
        }

        return { status: "error", error: "Failed to get the final response" };
    } catch (error) {
        return { status: "error", error: error instanceof Error ? error.message : "Unknown error" };
    }
}
