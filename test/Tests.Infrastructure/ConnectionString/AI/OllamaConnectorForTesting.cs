using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public abstract class AbstractOllamaConnectorForTesting<T, TConfig> : BaseAiConnectorForTesting<T, TConfig>
    where T : AbstractOllamaConnectorForTesting<T, TConfig>, new()
    where TConfig : AbstractAiIntegrationConfiguration, new()
{
    private const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_OLLAMA_URI";
    public abstract string Model { get; }

    public AbstractOllamaConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariable];
    }
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.Ollama);

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var uri = Environment.GetEnvironmentVariable(EnvironmentVariable);

        return new AiConnectionString
        {
            OllamaSettings = new OllamaSettings(uri, Model)
        };
    }
}

public class EmbeddingsOllamaConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsOllamaConnectorForTesting>
{
    public const string Model = "phi:latest";

    public EmbeddingsOllamaConnectorForTesting()
    {
        RequiredEnvironmentVariables = [OllamaConnectorHelper.EnvironmentVariable];
    }
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.Ollama);

    protected override AiConnectionString CreateAiConnectionStringImpl() => OllamaConnectorHelper.CreateAiConnectionString(Model, AiModelType.TextEmbeddings);
}

public class GenAiOllamaConnectorForTesting : AbstractGenAiConnectorForTesting<GenAiOllamaConnectorForTesting>
{
    public const string Model = "llama3.2:latest";

    public GenAiOllamaConnectorForTesting()
    {
        RequiredEnvironmentVariables = [OllamaConnectorHelper.EnvironmentVariable];
    }
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.Ollama);

    protected override AiConnectionString CreateAiConnectionStringImpl() => OllamaConnectorHelper.CreateAiConnectionString(Model, AiModelType.Chat);
}

internal static class OllamaConnectorHelper
{
    public const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_OLLAMA_URI";

    public static AiConnectionString CreateAiConnectionString(string model, AiModelType modelType)
    {
        var uri = Environment.GetEnvironmentVariable(EnvironmentVariable);

        return new AiConnectionString
        {
            ModelType = modelType,
            OllamaSettings = new OllamaSettings(uri, model)
        };
    }
}
