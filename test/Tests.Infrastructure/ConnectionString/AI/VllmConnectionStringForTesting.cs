using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsVllmConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsVllmConnectorForTesting>
{
    public EmbeddingsVllmConnectorForTesting()
    {
        RequiredEnvironmentVariables =
        [
            RavenTestHelper.EnvironmentVariables.AiIntegrationVllmApiKeyEnvName,
            RavenTestHelper.EnvironmentVariables.AiIntegrationVllmEmbEndpointEnvName,
            RavenTestHelper.EnvironmentVariables.AiIntegrationVllmEmbModelEnvName
        ];
        NamePrefix = nameof(RavenAiIntegration.vLLM);
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.OpenAi;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        return VllmConnectorHelper.CreateAiConnectionString(
            RavenTestHelper.EnvironmentVariables.AiIntegrationVllmApiKey,
            RavenTestHelper.EnvironmentVariables.AiIntegrationVllmEmbEndpoint,
            RavenTestHelper.EnvironmentVariables.AiIntegrationVllmEmbModel,
            AiModelType.TextEmbeddings);
    }
}

public class GenAiVllmConnectorForTesting : AbstractGenAiConnectorForTesting<GenAiVllmConnectorForTesting>
{
    public GenAiVllmConnectorForTesting()
    {
        RequiredEnvironmentVariables =
        [
            RavenTestHelper.EnvironmentVariables.AiIntegrationVllmApiKeyEnvName,
            RavenTestHelper.EnvironmentVariables.AiIntegrationVllmChatEndpointEnvName,
            RavenTestHelper.EnvironmentVariables.AiIntegrationVllmChatModelEnvName
        ];
        NamePrefix = nameof(RavenAiIntegration.vLLM);
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.OpenAi;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        return VllmConnectorHelper.CreateAiConnectionString(
            RavenTestHelper.EnvironmentVariables.AiIntegrationVllmApiKey,
            RavenTestHelper.EnvironmentVariables.AiIntegrationVllmChatEndpoint,
            RavenTestHelper.EnvironmentVariables.AiIntegrationVllmChatModel,
            AiModelType.Chat);
    }
}

internal static class VllmConnectorHelper
{
    public static AiConnectionString CreateAiConnectionString(string apiKey, string endpoint, string model, AiModelType modelType)
    {
        return new AiConnectionString
        {
            ModelType = modelType,
            OpenAiSettings = new OpenAiSettings(apiKey, endpoint, model) { Temperature = 0 }
        };
    }
}
