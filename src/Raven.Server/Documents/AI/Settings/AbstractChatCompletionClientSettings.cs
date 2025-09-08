using System;
using System.Net.Http;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

internal abstract class AbstractChatCompletionClientSettings
{
    private string _baseUri;
    
    public string ApiKey { get; }
    
    public string Model { get; }

    protected AbstractChatCompletionClientSettings(string baseUri, string apiKey, string model)
    {
        _baseUri = baseUri ?? throw new ArgumentNullException(nameof(baseUri));
        ApiKey = apiKey;
        Model = model;
    }

    internal void SetBaseUri(string baseUri)
    {
        _baseUri = baseUri ?? throw new ArgumentNullException(nameof(baseUri));
    }

    public virtual Uri GetBaseUri()
    {
        var baseUri = _baseUri;
        if (baseUri.EndsWith("/") == false)
            baseUri += "/";

        return new Uri(baseUri);
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

    internal static bool IsBaseUrl(Uri uri) =>
        uri.AbsolutePath == "/" 
        && string.IsNullOrEmpty(uri.Query) 
        && string.IsNullOrEmpty(uri.Fragment);
}
