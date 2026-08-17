using System;
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

    // The first request also loads the model into the GPU, which can take far longer than a hosted provider
    // needs to answer. Only this connector waits that long.
    protected override TimeSpan ConnectionProbeTimeout => TimeSpan.FromSeconds(120);

    public override GenAiConfiguration GetAiConfiguration()
    {
        var configuration = base.GetAiConfiguration();

        // A single local GPU serializes completions; parallel requests just accumulate wait time until they time out.
        configuration.MaxConcurrency = 1;

        return configuration;
    }

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
