using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsOpenAiConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsOpenAiConnectorForTesting>
{
    private const string Model = "text-embedding-3-small";

    public EmbeddingsOpenAiConnectorForTesting()
    {
        RequiredEnvironmentVariables = [RavenTestHelper.EnvironmentVariables.AiIntegrationOpenAiApiKeyKey];
    }
    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.OpenAi;

    protected override AiConnectionString CreateAiConnectionStringImpl() => OpenAiConnectorHelper.CreateAiConnectionString(Model, AiModelType.TextEmbeddings);
}

public class GenAiOpenAiConnectorForTesting : AbstractGenAiConnectorForTesting<GenAiOpenAiConnectorForTesting>
{
    private const string Model = "gpt-5-mini";

    public GenAiOpenAiConnectorForTesting()
    {
        RequiredEnvironmentVariables = [RavenTestHelper.EnvironmentVariables.AiIntegrationOpenAiApiKeyKey];
    }
    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.OpenAi;

    protected override AiConnectionString CreateAiConnectionStringImpl() => OpenAiConnectorHelper.CreateAiConnectionString(Model, AiModelType.Chat);

}

internal static class OpenAiConnectorHelper
{
    public const string Endpoint = "https://api.openai.com/";

    public static AiConnectionString CreateAiConnectionString(string model, AiModelType modelType)
    {
        return new AiConnectionString
        {
            ModelType = modelType,
            OpenAiSettings = new OpenAiSettings(RavenTestHelper.EnvironmentVariables.AiIntegrationOpenAiApiKey, Endpoint, model, reasoningEffort: OpenAiReasoningEffort.Minimal, seed: 48)
        };
    }
}
