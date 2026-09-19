import type {
    AiConnectionString,
    AiConnectionStringResponse,
    AiConnectionStringTestResponse,
} from "@/api/generated/server-api";
import { getServerConnectionStringName } from "@/components/ai-connection-string/ai-connection-string-utils";
import { apiHttp } from "./api-http";

export const aiConnectionStringsMocks = {
    list: (connectionStrings: AiConnectionStringResponse[] = sampleConnectionStringResponses) =>
        apiHttp.get("/api/ai/connection-strings", ({ response }) => response(200).json(connectionStrings)),
    detail: (connectionStrings: AiConnectionStringResponse[] = sampleConnectionStringResponses) =>
        apiHttp.get("/api/ai/connection-strings/{name}", ({ params, response }) => {
            const match = connectionStrings.find((item) => item.connectionString.name === params.name);
            if (!match) {
                return response(404).json({ error: `connection string '${params.name}' not found` });
            }
            return response(200).json({
                connectionString: { ...match.connectionString, name: params.name, identifier: params.name },
                usedBy: match.usedBy,
            });
        }),
    create: () =>
        apiHttp.post("/api/ai/connection-strings", async ({ request, response }) => {
            const connectionString = await request.json();
            return response(200).json({ name: connectionString.name ?? "connection-string" });
        }),
    test: (result: AiConnectionStringTestResponse = { success: true }) =>
        apiHttp.post("/api/ai/connection-strings/test", ({ response }) => response(200).json(result)),
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

export const sampleConnectionStringResponses: AiConnectionStringResponse[] = sampleConnectionStrings.map(
    (connectionString) => ({ connectionString, usedBy: [] }),
);

export const sampleUsedByAgentsConnectionStringResponses: AiConnectionStringResponse[] = [
    {
        connectionString: sampleChatConnectionString,
        usedBy: [
            { kind: "AiAgent", identifier: "support-agent", name: "Support agent", databaseName: "northwind" },
            { kind: "AiAgent", identifier: "sales-agent", name: null, databaseName: "crm" },
        ],
    },
    ...sampleConnectionStringResponses.slice(1),
];

export const sampleUsedByTasksConnectionStringResponses: AiConnectionStringResponse[] = [
    {
        connectionString: sampleChatConnectionString,
        usedBy: [
            { kind: "AiAgent", identifier: "support-agent", name: "Support agent", databaseName: "northwind" },
            { kind: "GenAi", identifier: "12", name: "Summarize orders", databaseName: "northwind" },
        ],
    },
    ...sampleConnectionStringResponses.slice(1),
];

export const sampleUsedByTasksVertexConnectionStringResponses: AiConnectionStringResponse[] = [
    {
        connectionString: {
            name: "vertex-embeddings",
            identifier: "vertex-embeddings",
            modelType: "TextEmbeddings",
            vertexSettings: {
                googleCredentialsJson: '{ "type": "service_account" }',
                model: "gemini-embedding-001",
                location: "us-central1",
                aiVersion: "V1",
            },
        },
        usedBy: [{ kind: "EmbeddingsGeneration", identifier: "7", name: "Index products", databaseName: "northwind" }],
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
