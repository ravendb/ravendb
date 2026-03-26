using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsVllmConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsVllmConnectorForTesting>
{
    private const string EnvironmentVariableApiKey = "RAVEN_AI_INTEGRATION_VLLM_API_KEY";
    private const string EnvironmentVariableEndpoint = "RAVEN_AI_INTEGRATION_VLLM_EMB_ENDPOINT";
    private const string EnvironmentVariableModelName = "RAVEN_AI_INTEGRATION_VLLM_EMB_MODEL";
    public EmbeddingsVllmConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariableApiKey, EnvironmentVariableEndpoint, EnvironmentVariableModelName];
        NamePrefix = nameof(RavenAiIntegration.vLLM);
    }
    
    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.OpenAi;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariableApiKey);
        var endpoint = Environment.GetEnvironmentVariable(EnvironmentVariableEndpoint);
        var model = Environment.GetEnvironmentVariable(EnvironmentVariableModelName);

        return VllmConnectorHelper.CreateAiConnectionString(apiKey, endpoint, model, AiModelType.TextEmbeddings);
    }
}

public class GenAiVllmConnectorForTesting : AbstractGenAiConnectorForTesting<GenAiVllmConnectorForTesting>
{
    private const string EnvironmentVariableApiKey = "RAVEN_AI_INTEGRATION_VLLM_API_KEY";
    private const string EnvironmentVariableEndpoint = "RAVEN_AI_INTEGRATION_VLLM_CHAT_ENDPOINT";
    private const string EnvironmentVariableModelName = "RAVEN_AI_INTEGRATION_VLLM_CHAT_MODEL";

    public GenAiVllmConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariableApiKey, EnvironmentVariableEndpoint, EnvironmentVariableModelName];
        NamePrefix = nameof(RavenAiIntegration.vLLM);
    }

    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.OpenAi;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariableApiKey);
        var endpoint = Environment.GetEnvironmentVariable(EnvironmentVariableEndpoint);
        var model = Environment.GetEnvironmentVariable(EnvironmentVariableModelName);

        return VllmConnectorHelper.CreateAiConnectionString(apiKey, endpoint, model, AiModelType.Chat);
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
