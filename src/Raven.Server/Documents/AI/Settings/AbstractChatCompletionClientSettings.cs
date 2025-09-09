using System;
using System.Net.Http;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

internal abstract class AbstractChatCompletionClientSettings
{
    private readonly IChatCompletionSettings _settings;
    
    public string ApiKey => _settings.ApiKey;

    public string Model => _settings.Model;

    public Uri GetBaseEndpointUri() => _settings.GetBaseEndpointUri();

    protected AbstractChatCompletionClientSettings(IChatCompletionSettings settings)
    {
        _settings = settings;
    }

    public abstract void HandleCompletionRequestPayload(AsyncBlittableJsonTextWriter writer);

    public virtual void AddHeaders(HttpRequestMessage request)
    {
    }

    public virtual string GetRelativeCompletionUri() => "chat/completions";

    public virtual string GetRelativeModelsUri() => "models";
    
    internal static bool TryGetParameters(AiConnectionString connectionString, out AbstractChatCompletionClientSettings settings)
    {
        settings = null;

        switch (connectionString.ModelType)
        {
            case AiModelType.Chat:
                break;
            default:
                throw new InvalidOperationException(
                    $"Invalid provider settings for '{connectionString.Name}' with model type '{connectionString.ModelType}'. " +
                    $"Supported providers for '{nameof(connectionString.ModelType.Chat)}' model type are '{nameof(AiConnectorType.OpenAi)}', '{nameof(AiConnectorType.Ollama)}' and '{nameof(AiConnectorType.AzureOpenAi)}'");
        }

        var provider = connectionString.GetActiveProvider();
        switch (provider)
        {
            case AiConnectorType.OpenAi:
                settings = new OpenAiChatCompletionClientSettings(connectionString.OpenAiSettings);
                return true;
            case AiConnectorType.AzureOpenAi:
                settings = new AzureOpenAiChatCompletionClientSettings(connectionString.AzureOpenAiSettings);
                return true;
            case AiConnectorType.Ollama:
                settings = new OllamaChatCompletionClientSettings(connectionString.OllamaSettings);
                return true;
        }

        return false;
    }
    
    protected static class Constants
    {
        public static class RequestFields
        {
            public const string Think = "think";
            public const string Temperature = "temperature";
        }

        public static class Headers
        {
            public const string OpenAiOrganization = "OpenAI-Organization";
            public const string OpenAiProject = "OpenAI-Project";
        }
    }
}
