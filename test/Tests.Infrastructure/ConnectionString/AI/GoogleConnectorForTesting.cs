using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsGoogleConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsGoogleConnectorForTesting>
{
    private const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_GOOGLE_API_KEY";
    private const string Model = "text-embedding-004";

    public EmbeddingsGoogleConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariable];
    }
    
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.Google);

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariable);
        return new AiConnectionString
        {
            ModelType = AiModelType.Embeddings,
            GoogleSettings = new GoogleSettings(Model, apiKey)
        };
    }
}
