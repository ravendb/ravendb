using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsOllamaConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsOllamaConnectorForTesting>
{
    public const string Model = "phi:latest";
    public const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_OLLAMA_EMB_URI";

    public EmbeddingsOllamaConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariable];
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.Ollama;

    protected override AiConnectionString CreateAiConnectionStringImpl() => OllamaConnectorHelper.CreateAiConnectionString(Model, AiModelType.TextEmbeddings, EnvironmentVariable);
}

public class GenAiOllamaConnectorForTesting : AbstractGenAiConnectorForTesting<GenAiOllamaConnectorForTesting>
{
    public const string Model = "llama3.2:latest";
    public const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_OLLAMA_CHAT_URI";

    public GenAiOllamaConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariable];
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.Ollama;

    protected override AiConnectionString CreateAiConnectionStringImpl() => OllamaConnectorHelper.CreateAiConnectionString(Model, AiModelType.Chat, EnvironmentVariable);
}

internal static class OllamaConnectorHelper
{
    public static AiConnectionString CreateAiConnectionString(string model, AiModelType modelType, string environmentVariable)
    {
        var uri = Environment.GetEnvironmentVariable(environmentVariable);

        return new AiConnectionString
        {
            ModelType = modelType,
            OllamaSettings = new OllamaSettings(uri, model) { Temperature = 0 }
        };
    }
}
