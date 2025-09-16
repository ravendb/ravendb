using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsVertexConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsVertexConnectorForTesting>
{
    private const string EnvironmentVariableApiKey = "RAVEN_AI_INTEGRATION_VERTEX_API_KEY";
    private const string EnvironmentVariableLocation = "RAVEN_AI_INTEGRATION_VERTEX_LOCATION";
    private const string EnvironmentVariableProjectId = "RAVEN_AI_INTEGRATION_VERTEX_PROJECT_ID";
    private const string Model = "text-embedding-005";

    public EmbeddingsVertexConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariableApiKey, EnvironmentVariableLocation, EnvironmentVariableProjectId];
    }
    
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.Vertex);

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariableApiKey);
        var location = Environment.GetEnvironmentVariable(EnvironmentVariableLocation);
        var projectId = Environment.GetEnvironmentVariable(EnvironmentVariableProjectId);
        
        return new AiConnectionString
        {
            ModelType = AiModelType.TextEmbeddings,
            VertexSettings = new VertexSettings(Model, apiKey, location, projectId, VertexAIVersion.V1)
        };
    }
}
