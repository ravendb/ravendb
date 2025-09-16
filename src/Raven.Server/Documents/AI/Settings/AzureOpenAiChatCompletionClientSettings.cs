using Raven.Client.Documents.Operations.AI;

namespace Raven.Server.Documents.AI.Settings;

internal class AzureOpenAiChatCompletionClientSettings : AbstractOpenAiChatCompletionClientSettings
{
    private readonly AzureOpenAiSettings _settings;

    private const string ApiVersion = "2024-10-21";

    public AzureOpenAiChatCompletionClientSettings(AzureOpenAiSettings settings)
        : base(settings)
    {
        _settings = settings;
    }

    public override string GetRelativeCompletionUri() => $"openai/deployments/{_settings.DeploymentName}/chat/completions?api-version={ApiVersion}";

    public override string GetRelativeModelsUri() => $"openai/models?api-version={ApiVersion}";
}
