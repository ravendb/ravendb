using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public sealed class EmbeddingsAzureOpenAiConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsAzureOpenAiConnectorForTesting>
{
    private const string Model = "text-embedding-3-small";

    public EmbeddingsAzureOpenAiConnectorForTesting()
    {
        RequiredEnvironmentVariables =
        [
            RavenTestHelper.EnvironmentVariables.AiIntegrationAzureOpenAiApiKeyKey,
            RavenTestHelper.EnvironmentVariables.AiIntegrationAzureOpenAiDeploymentEndpointKey,
            RavenTestHelper.EnvironmentVariables.AiIntegrationAzureOpenAiDeploymentNameKey
        ];
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.AzureOpenAi;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        return AzureOpenAiConnectorHelper.CreateAiConnectionString(
            RavenTestHelper.EnvironmentVariables.AiIntegrationAzureOpenAiApiKey,
            RavenTestHelper.EnvironmentVariables.AiIntegrationAzureOpenAiDeploymentEndpoint,
            Model,
            RavenTestHelper.EnvironmentVariables.AiIntegrationAzureOpenAiDeploymentName,
            AiModelType.TextEmbeddings);
    }
}

public class GenAiAzureOpenAiConnectorForTesting : AbstractGenAiConnectorForTesting<GenAiAzureOpenAiConnectorForTesting>
{
    private const string Model = "gpt-4.1-mini";

    public GenAiAzureOpenAiConnectorForTesting()
    {
        RequiredEnvironmentVariables =
        [
            RavenTestHelper.EnvironmentVariables.AiIntegrationAzureOpenAiApiKeyKey,
            RavenTestHelper.EnvironmentVariables.AiIntegrationAzureOpenAiDeploymentEndpointKey,
            RavenTestHelper.EnvironmentVariables.AiIntegrationAzureOpenAiChatDeploymentNameKey
        ];
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.AzureOpenAi;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        return AzureOpenAiConnectorHelper.CreateAiConnectionString(
            RavenTestHelper.EnvironmentVariables.AiIntegrationAzureOpenAiApiKey,
            RavenTestHelper.EnvironmentVariables.AiIntegrationAzureOpenAiDeploymentEndpoint,
            Model,
            RavenTestHelper.EnvironmentVariables.AiIntegrationAzureOpenAiChatDeploymentName,
            AiModelType.Chat);
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
