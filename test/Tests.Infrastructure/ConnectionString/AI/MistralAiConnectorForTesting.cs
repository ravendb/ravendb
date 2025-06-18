using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsMistralAiConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsMistralAiConnectorForTesting>
{
    private const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_MISTRAL_API_KEY";
    private const string Endpoint = "https://api.mistral.ai/v1";
    private const string Model = "mistral-embed";

    public EmbeddingsMistralAiConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariable];
    }
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.MistralAi);

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariable);

        return new AiConnectionString
        {
            ModelType = AiModelType.Embeddings,
            MistralAiSettings = new MistralAiSettings(Model, apiKey, Endpoint)
        };
    }
}
