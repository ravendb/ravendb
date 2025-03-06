using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public sealed class AzureOpenAiConnectorForTesting : BaseAiConnectorForTesting<AzureOpenAiConnectorForTesting>
{
    private const string EnvironmentVariableApiKey = "RAVEN_AI_INTEGRATION_AZURE_OPENAI_API_KEY";
    private const string EnvironmentVariableDeploymentEndpoint = "RAVEN_AI_INTEGRATION_AZURE_OPENAI_DEPLOYMENT_ENDPOINT";
    private const string EnvironmentVariableDeploymentName = "RAVEN_AI_INTEGRATION_AZURE_OPENAI_DEPLOYMENT_NAME";
    private const string Model = "text-embedding-3-small";

    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.AzureOpenAi);

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariableApiKey);
        var endpoint = Environment.GetEnvironmentVariable(EnvironmentVariableDeploymentEndpoint);
        var deploymentName = Environment.GetEnvironmentVariable(EnvironmentVariableDeploymentName);

        return new AiConnectionString
        {
            AzureOpenAiSettings = new AzureOpenAiSettings(apiKey, endpoint, Model, deploymentName)
        };
    }
}
