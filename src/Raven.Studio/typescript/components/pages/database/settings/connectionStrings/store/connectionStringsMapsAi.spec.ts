import _ from "lodash";
import { mapAiConnectionsFromDto } from "./connectionStringsMapsFromDto";
import { mapAiConnectionStringToDto } from "./connectionStringsMapsToDto";

type AiConnectionStringDto = Raven.Client.Documents.Operations.AI.AiConnectionString;

const anthropicDto: AiConnectionStringDto = {
    Type: "Ai",
    Name: "claude",
    Identifier: "claude",
    ModelType: "Chat",
    UsedBy: [],
    AnthropicSettings: {
        ApiKey: "sk-ant-key",
        Model: "claude-opus-5",
        Endpoint: "https://api.anthropic.com/v1/",
        MaxOutputTokens: 32000,
        ReasoningEffort: "high",
        EnablePromptCache: false,
        ApiVersion: "2023-06-01",
        EmbeddingsMaxConcurrentBatches: null,
    },
    AzureOpenAiSettings: null,
    EmbeddedSettings: null,
    GoogleSettings: null,
    HuggingFaceSettings: null,
    MistralAiSettings: null,
    OllamaSettings: null,
    OpenAiSettings: null,
    VertexSettings: null,
};

function chatDtoWith(settings: Partial<AiConnectionStringDto>): AiConnectionStringDto {
    return { ...anthropicDto, AnthropicSettings: null, ...settings };
}

function saveAfterLoad(dto: AiConnectionStringDto): AiConnectionStringDto {
    const [connection] = mapAiConnectionsFromDto({ [dto.Name]: dto });
    return mapAiConnectionStringToDto(connection);
}

describe("AI connection string maps", () => {
    it("round-trips Anthropic settings", () => {
        const [connection] = mapAiConnectionsFromDto({ claude: anthropicDto });

        expect(connection.connectorType).toBe("anthropicSettings");
        expect(mapAiConnectionStringToDto(connection)).toEqual(_.omit(anthropicDto, "UsedBy"));
    });

    it("keeps OpenAI settings that have no Studio field", () => {
        const dto = chatDtoWith({
            OpenAiSettings: {
                ApiKey: "sk-key",
                Endpoint: "https://api.openai.com/v1/",
                Model: "gpt-5",
                OrganizationId: null,
                ProjectId: null,
                ReasoningEffort: "high",
                ApiMode: "ChatCompletions",
            },
        });

        expect(saveAfterLoad(dto).OpenAiSettings.ApiMode).toBe("ChatCompletions");
    });

    it("keeps Azure OpenAI settings that have no Studio field", () => {
        const dto = chatDtoWith({
            AzureOpenAiSettings: {
                ApiKey: "azure-key",
                Endpoint: "https://example.openai.azure.com/",
                Model: "gpt-5",
                DeploymentName: "gpt-5",
                ReasoningEffort: "low",
                ApiMode: "Responses",
            },
        });

        expect(saveAfterLoad(dto).AzureOpenAiSettings).toMatchObject({ ReasoningEffort: "low", ApiMode: "Responses" });
    });

    it("keeps Ollama settings that have no Studio field", () => {
        const dto = chatDtoWith({
            OllamaSettings: {
                ApiKey: "ollama-key",
                Endpoint: "http://localhost:11434",
                Uri: "http://localhost:11434",
                Model: "llama3",
                ReasoningEffort: "medium",
                ApiMode: "Responses",
            },
        });

        expect(saveAfterLoad(dto).OllamaSettings).toMatchObject({
            ApiKey: "ollama-key",
            ReasoningEffort: "medium",
            ApiMode: "Responses",
        });
    });
});
