import type { AiConnectionString, AiConnectionStringListResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const aiConnectionStringsMocks = {
    list: (connectionStrings: AiConnectionStringListResponse = sampleConnectionStrings) =>
        apiHttp.get("/api/apps/{slug}/ai/connection-strings", ({ response }) => response(200).json(connectionStrings)),
    detail: (connectionString: AiConnectionString = sampleChatConnectionString) =>
        apiHttp.get("/api/apps/{slug}/ai/connection-strings/{name}", ({ params, response }) =>
            response(200).json({ ...connectionString, name: params.name, identifier: params.name }),
        ),
    create: () =>
        apiHttp.post("/api/apps/{slug}/ai/connection-strings", async ({ request, response }) => {
            const connectionString = await request.json();
            return response(200).json({ name: connectionString.name ?? "connection-string" });
        }),
    delete: () =>
        apiHttp.delete("/api/apps/{slug}/ai/connection-strings/{name}", ({ response }) => response(204).empty()),
};

export const sampleConnectionStrings: AiConnectionStringListResponse = {
    items: [
        {
            name: "openai-chat",
            identifier: "openai-chat",
            modelType: "Chat",
            provider: "OpenAi",
        },
        {
            name: "embeddings",
            identifier: "embeddings",
            modelType: "TextEmbeddings",
            provider: "Embedded",
        },
    ],
};

export const sampleChatConnectionString: AiConnectionString = {
    name: "openai-chat",
    identifier: "openai-chat",
    modelType: "Chat",
    openAiSettings: {
        apiKey: "sk-mock-key",
        model: "gpt-4o-mini",
        temperature: 0.2,
    },
};
