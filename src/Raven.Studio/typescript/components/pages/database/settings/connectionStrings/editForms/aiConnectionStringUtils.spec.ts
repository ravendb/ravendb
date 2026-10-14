import { aiConnectionStringUtils } from "./aiConnectionStringUtils";
import { AiConnection, ConnectionFormData } from "../connectionStringsTypes";

type FormData = ConnectionFormData<AiConnection>;

const anthropicConnection: FormData = {
    name: "claude",
    identifier: null,
    modelType: "Chat",
    connectorType: "anthropicSettings",
    anthropicSettings: {
        apiKey: "sk-ant-key",
        model: "claude-opus-5",
        endpoint: null,
        maxOutputTokens: null,
        reasoningEffort: null,
        enablePromptCache: null,
        apiVersion: null,
    },
};

const isValid = (data: FormData) =>
    aiConnectionStringUtils.schema.isValid(data, {
        context: {
            connectorType: data.connectorType,
            modelType: data.modelType,
            isForNewConnection: true,
            usedNames: [],
        },
    });

const withAnthropicSettings = (settings: Partial<FormData["anthropicSettings"]>): FormData => ({
    ...anthropicConnection,
    anthropicSettings: { ...anthropicConnection.anthropicSettings, ...settings },
});

describe("aiConnectionStringUtils", () => {
    describe("getConnectorOptions", () => {
        const optionValues = (modelType: FormData["modelType"]) =>
            aiConnectionStringUtils.getConnectorOptions(modelType).map((x) => x.value);

        it("offers Anthropic for chat models", () => {
            expect(optionValues("Chat")).toContain("anthropicSettings");
        });

        it("hides Anthropic for text embeddings because it has no embeddings API", () => {
            expect(optionValues("TextEmbeddings")).not.toContain("anthropicSettings");
            expect(optionValues("TextEmbeddings")).toContain("openAiSettings");
        });
    });

    describe("Anthropic schema", () => {
        it("accepts a connection with API key and model only", async () => {
            await expect(isValid(anthropicConnection)).resolves.toBe(true);
        });

        it("requires an API key", async () => {
            await expect(isValid(withAnthropicSettings({ apiKey: null }))).resolves.toBe(false);
            await expect(isValid(withAnthropicSettings({ apiKey: "   " }))).resolves.toBe(false);
        });

        it("requires a model", async () => {
            await expect(isValid(withAnthropicSettings({ model: null }))).resolves.toBe(false);
        });

        it("requires max output tokens to be a positive integer when set", async () => {
            await expect(isValid(withAnthropicSettings({ maxOutputTokens: 0 }))).resolves.toBe(false);
            await expect(isValid(withAnthropicSettings({ maxOutputTokens: 1.5 }))).resolves.toBe(false);
            await expect(isValid(withAnthropicSettings({ maxOutputTokens: 32000 }))).resolves.toBe(true);
        });

        it("ignores Anthropic fields when another connector is selected", async () => {
            const openAiConnection: FormData = {
                ...withAnthropicSettings({ apiKey: null, model: null, maxOutputTokens: 0 }),
                connectorType: "openAiSettings",
                openAiSettings: { apiKey: "sk-key", model: "gpt-5" },
            };

            await expect(isValid(openAiConnection)).resolves.toBe(true);
        });
    });
});
