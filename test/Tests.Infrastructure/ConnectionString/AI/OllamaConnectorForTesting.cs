using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsOllamaConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsOllamaConnectorForTesting>
{
    public const string Model = "nomic-embed-text:latest";

    public EmbeddingsOllamaConnectorForTesting()
    {
        RequiredEnvironmentVariables = [RavenTestHelper.EnvironmentVariables.AiIntegrationOllamaEmbUriEnvName];
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.Ollama;

    protected override AiConnectionString CreateAiConnectionStringImpl() => OllamaConnectorHelper.CreateAiConnectionString(Model, AiModelType.TextEmbeddings, RavenTestHelper.EnvironmentVariables.AiIntegrationOllamaEmbUri);
}

public class GenAiOllamaConnectorForTesting : AbstractGenAiConnectorForTesting<GenAiOllamaConnectorForTesting>
{
    public const string Model = "qwen2.5:0.5b";

    public GenAiOllamaConnectorForTesting()
    {
        RequiredEnvironmentVariables = [RavenTestHelper.EnvironmentVariables.AiIntegrationOllamaChatUriEnvName];
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.Ollama;

    protected override AiConnectionString CreateAiConnectionStringImpl() => OllamaConnectorHelper.CreateAiConnectionString(Model, AiModelType.Chat, RavenTestHelper.EnvironmentVariables.AiIntegrationOllamaChatUri);
}

internal static class OllamaConnectorHelper
{
    public static AiConnectionString CreateAiConnectionString(string model, AiModelType modelType, string uri)
    {
        return new AiConnectionString
        {
            ModelType = modelType,
            OllamaSettings = new OllamaSettings(uri, model) { Temperature = 0 }
        };
    }
}
