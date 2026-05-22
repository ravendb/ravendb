using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsHuggingFaceConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsHuggingFaceConnectorForTesting>
{
    private const string Model = "sentence-transformers/all-MiniLM-L6-v2";

    public EmbeddingsHuggingFaceConnectorForTesting()
    {
        RequiredEnvironmentVariables = [RavenTestHelper.EnvironmentVariables.AiIntegrationHuggingFaceApiKeyEnvName];
    }
    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.HuggingFace;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        return new AiConnectionString
        {
            ModelType = AiModelType.TextEmbeddings,
            HuggingFaceSettings = new HuggingFaceSettings(RavenTestHelper.EnvironmentVariables.AiIntegrationHuggingFaceApiKey, Model)
        };
    }
}
