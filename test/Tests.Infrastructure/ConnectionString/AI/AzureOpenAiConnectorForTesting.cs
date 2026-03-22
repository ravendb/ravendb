using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public sealed class EmbeddingsAzureOpenAiConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsAzureOpenAiConnectorForTesting>
{
    private const string EnvironmentVariableApiKey = "RAVEN_AI_INTEGRATION_AZURE_OPENAI_API_KEY";
    private const string EnvironmentVariableDeploymentEndpoint = "RAVEN_AI_INTEGRATION_AZURE_OPENAI_DEPLOYMENT_ENDPOINT";
    private const string EnvironmentVariableDeploymentName = "RAVEN_AI_INTEGRATION_AZURE_OPENAI_DEPLOYMENT_NAME";
    private const string Model = "text-embedding-3-small";

    public EmbeddingsAzureOpenAiConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariableApiKey, EnvironmentVariableDeploymentEndpoint, EnvironmentVariableDeploymentName];
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.AzureOpenAi;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariableApiKey);
        var endpoint = Environment.GetEnvironmentVariable(EnvironmentVariableDeploymentEndpoint);
        var deploymentName = Environment.GetEnvironmentVariable(EnvironmentVariableDeploymentName);

        return AzureOpenAiConnectorHelper.CreateAiConnectionString(apiKey, endpoint, Model, deploymentName, AiModelType.TextEmbeddings);
    }
}

public class GenAiAzureOpenAiConnectorForTesting : AbstractGenAiConnectorForTesting<GenAiAzureOpenAiConnectorForTesting>
{
    private const string EnvironmentVariableApiKey = "RAVEN_AI_INTEGRATION_AZURE_OPENAI_API_KEY";
    private const string EnvironmentVariableDeploymentEndpoint = "RAVEN_AI_INTEGRATION_AZURE_OPENAI_DEPLOYMENT_ENDPOINT";
    private const string EnvironmentVariableDeploymentName = "RAVEN_AI_INTEGRATION_AZURE_OPENAI_CHAT_DEPLOYMENT_NAME";
    private const string Model = "gpt-4.1-mini";

    public GenAiAzureOpenAiConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariableApiKey, EnvironmentVariableDeploymentEndpoint, EnvironmentVariableDeploymentName];
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.AzureOpenAi;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariableApiKey);
        var endpoint = Environment.GetEnvironmentVariable(EnvironmentVariableDeploymentEndpoint);
        var deploymentName = Environment.GetEnvironmentVariable(EnvironmentVariableDeploymentName);

        return AzureOpenAiConnectorHelper.CreateAiConnectionString(apiKey, endpoint, Model, deploymentName, AiModelType.Chat);
    }
}

internal static class AzureOpenAiConnectorHelper
{
    public static AiConnectionString CreateAiConnectionString(string apiKey, string endpoint, string model, string deploymentName, AiModelType modelType)
    {
        return new AiConnectionString
        {
            ModelType = modelType,
            AzureOpenAiSettings = new AzureOpenAiSettings(apiKey, endpoint, model, deploymentName) { Temperature = 0 }
        };
    }
}
