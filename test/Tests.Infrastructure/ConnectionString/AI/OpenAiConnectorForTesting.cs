using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsOpenAiConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsOpenAiConnectorForTesting>
{
    private const string Model = "text-embedding-3-small";

    public EmbeddingsOpenAiConnectorForTesting()
    {
        RequiredEnvironmentVariables = [OpenAiConnectorHelper.EnvironmentVariable];
    }
    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.OpenAi;

    protected override AiConnectionString CreateAiConnectionStringImpl() => OpenAiConnectorHelper.CreateAiConnectionString(Model, AiModelType.TextEmbeddings);
}

public class GenAiOpenAiConnectorForTesting : AbstractGenAiConnectorForTesting<GenAiOpenAiConnectorForTesting>
{
    private const string Model = "gpt-5-mini";

    public GenAiOpenAiConnectorForTesting()
    {
        RequiredEnvironmentVariables = [OpenAiConnectorHelper.EnvironmentVariable];
    }
    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.OpenAi;

    protected override AiConnectionString CreateAiConnectionStringImpl() => OpenAiConnectorHelper.CreateAiConnectionString(Model, AiModelType.Chat);
    
}

internal static class OpenAiConnectorHelper
{
    public const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_OPENAI_API_KEY";
    public const string Endpoint = "https://api.openai.com/";

    public static AiConnectionString CreateAiConnectionString(string model, AiModelType modelType)
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariable);
        return new AiConnectionString
        {
            ModelType = modelType,
            OpenAiSettings = new OpenAiSettings(apiKey, Endpoint, model, reasoningEffort: OpenAiReasoningEffort.Minimal, seed: 48)
        };
    }
}
