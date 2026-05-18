using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsVertexConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsVertexConnectorForTesting>
{
    private const string Model = "text-embedding-005";

    public EmbeddingsVertexConnectorForTesting()
    {
        RequiredEnvironmentVariables =
        [
            RavenTestHelper.EnvironmentVariables.AiIntegrationVertexGoogleCredentialsJsonKey,
            RavenTestHelper.EnvironmentVariables.AiIntegrationVertexLocationKey
        ];
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.Vertex;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        return new AiConnectionString
        {
            ModelType = AiModelType.TextEmbeddings,
            VertexSettings = new VertexSettings(Model, RavenTestHelper.EnvironmentVariables.AiIntegrationVertexGoogleCredentialsJson, RavenTestHelper.EnvironmentVariables.AiIntegrationVertexLocation, VertexAIVersion.V1)
        };
    }
}
