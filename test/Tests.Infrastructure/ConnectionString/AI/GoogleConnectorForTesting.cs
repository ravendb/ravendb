using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsGoogleConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsGoogleConnectorForTesting>
{
    private const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_GOOGLE_API_KEY";
    private const string Model = "gemini-embedding-001";

    public EmbeddingsGoogleConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariable];
    }
    
    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.Google;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariable);
        return new AiConnectionString
        {
            ModelType = AiModelType.TextEmbeddings,
            GoogleSettings = new GoogleSettings(Model, apiKey)
        };
    }
}
