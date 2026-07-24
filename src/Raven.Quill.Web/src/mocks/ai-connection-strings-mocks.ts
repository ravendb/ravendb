import type { AiConnectionString } from "@/api/generated/server-api";
import { getServerConnectionStringName } from "@/components/ai-connection-string/ai-connection-string-utils";
import { apiHttp } from "./api-http";

export const aiConnectionStringsMocks = {
    list: (connectionStrings: AiConnectionString[] = sampleConnectionStrings) =>
        apiHttp.get("/api/ai/connection-strings", ({ response }) => response(200).json(connectionStrings)),
    detail: (connectionString: AiConnectionString = sampleChatConnectionString) =>
        apiHttp.get("/api/ai/connection-strings/{name}", ({ params, response }) =>
            response(200).json({ ...connectionString, name: params.name, identifier: params.name }),
        ),
    create: () =>
        apiHttp.post("/api/ai/connection-strings", async ({ request, response }) => {
            const connectionString = await request.json();
            return response(200).json({ name: connectionString.name ?? "connection-string" });
        }),
    delete: () => apiHttp.delete("/api/ai/connection-strings/{name}", ({ response }) => response(204).empty()),
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

export const sampleConnectionStrings: AiConnectionString[] = [
    sampleChatConnectionString,
    {
        name: "embeddings",
        identifier: "embeddings",
        modelType: "TextEmbeddings",
        embeddedSettings: {},
    },
];

// The per-app endpoint reads the database record, where server-wide connection
// strings appear under their propagated (prefixed) names — unlike the server-wide
// endpoints above, which use bare names.
export const samplePropagatedConnectionStrings: AiConnectionString[] = sampleConnectionStrings.map(
    (connectionString) => ({
        ...connectionString,
        name: getServerConnectionStringName(connectionString.name ?? ""),
    }),
);
