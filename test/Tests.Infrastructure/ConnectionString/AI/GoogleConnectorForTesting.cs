using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsGoogleConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsGoogleConnectorForTesting>
{
    private const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_GOOGLE_API_KEY";
    private const string Model = "gemini-embedding-001";
    public const string Endpoint = "https://generativelanguage.googleapis.com/";

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
            GoogleSettings = new GoogleSettings(Model, apiKey, Endpoint)
        };
    }
}

public class GenAiGoogleConnectorForTesting : AbstractGenAiConnectorForTesting<GenAiGoogleConnectorForTesting>
{
    public const string Model = "gemini-3-flash-preview";
    public const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_GOOGLE_API_KEY";
    public const string Endpoint = "https://generativelanguage.googleapis.com/";

    public GenAiGoogleConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariable];
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.Google;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariable);
        return new AiConnectionString
        {
            ModelType = AiModelType.Chat,
            GoogleSettings = new GoogleSettings(Model, apiKey, Endpoint, GoogleAIVersion.V1_Beta)
        };
    }
}
