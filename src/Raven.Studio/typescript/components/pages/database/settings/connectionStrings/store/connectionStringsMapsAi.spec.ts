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

describe("AI connection string maps", () => {
    it("round-trips Anthropic settings", () => {
        const [connection] = mapAiConnectionsFromDto({ claude: anthropicDto });

        expect(connection.connectorType).toBe("anthropicSettings");
        expect(mapAiConnectionStringToDto(connection)).toEqual(_.omit(anthropicDto, "UsedBy"));
    });
});
