using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsMistralAiConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsMistralAiConnectorForTesting>
{
    private const string Endpoint = "https://api.mistral.ai/v1";
    private const string Model = "mistral-embed";

    public EmbeddingsMistralAiConnectorForTesting()
    {
        RequiredEnvironmentVariables = [RavenTestHelper.EnvironmentVariables.AiIntegrationMistralApiKeyKey];
    }
    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.MistralAi;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        return new AiConnectionString
        {
            ModelType = AiModelType.TextEmbeddings,
            MistralAiSettings = new MistralAiSettings(Model, RavenTestHelper.EnvironmentVariables.AiIntegrationMistralApiKey, Endpoint)
        };
    }
}
