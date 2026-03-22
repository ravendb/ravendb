using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsVertexConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsVertexConnectorForTesting>
{
    private const string EnvironmentVariableGoogleCredentialsJson = "RAVEN_AI_INTEGRATION_VERTEX_GOOGLE_CREDENTIALS_JSON";
    private const string EnvironmentVariableLocation = "RAVEN_AI_INTEGRATION_VERTEX_LOCATION";
    private const string Model = "text-embedding-005";

    public EmbeddingsVertexConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariableGoogleCredentialsJson, EnvironmentVariableLocation];
    }
    
    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.Vertex;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var googleCredentialsJson = Environment.GetEnvironmentVariable(EnvironmentVariableGoogleCredentialsJson);
        var location = Environment.GetEnvironmentVariable(EnvironmentVariableLocation);
        
        return new AiConnectionString
        {
            ModelType = AiModelType.TextEmbeddings,
            VertexSettings = new VertexSettings(Model, googleCredentialsJson, location, VertexAIVersion.V1)
        };
    }
}
