using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsGoogleConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsGoogleConnectorForTesting>
{
    private const string Model = "gemini-embedding-001";
    public const string Endpoint = "https://generativelanguage.googleapis.com/";

    public EmbeddingsGoogleConnectorForTesting()
    {
        RequiredEnvironmentVariables = [RavenTestHelper.EnvironmentVariables.AiIntegrationGoogleApiKeyKey];
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.Google;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        return new AiConnectionString
        {
            ModelType = AiModelType.TextEmbeddings,
            GoogleSettings = new GoogleSettings(Model, RavenTestHelper.EnvironmentVariables.AiIntegrationGoogleApiKey, Endpoint)
        };
    }
}

public class GenAiGoogleConnectorForTesting : AbstractGenAiConnectorForTesting<GenAiGoogleConnectorForTesting>
{
    public const string Model = "gemini-3-flash-preview";
    public const string Endpoint = "https://generativelanguage.googleapis.com/";

    public GenAiGoogleConnectorForTesting()
    {
        RequiredEnvironmentVariables = [RavenTestHelper.EnvironmentVariables.AiIntegrationGoogleApiKeyKey];
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.Google;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        return new AiConnectionString
        {
            ModelType = AiModelType.Chat,
            GoogleSettings = new GoogleSettings(Model, RavenTestHelper.EnvironmentVariables.AiIntegrationGoogleApiKey, Endpoint, GoogleAIVersion.V1_Beta)
        };
    }
}
