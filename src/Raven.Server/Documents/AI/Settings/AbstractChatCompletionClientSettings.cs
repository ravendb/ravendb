using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI.Settings;

internal abstract class AbstractChatCompletionClientSettings
{
    private readonly IAiSettings _settings;
    
    public string ApiKey => _settings.ApiKey;

    public string Model => _settings.Model;

    public virtual bool SupportStrictTools => true;

    public virtual bool EnablePromptCaching => true;

    public Uri GetBaseEndpointUri() => _settings.GetBaseEndpointUri();

    protected AbstractChatCompletionClientSettings(IAiSettings settings)
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
                    $"Supported providers for '{nameof(connectionString.ModelType.Chat)}' model type are '{nameof(AiConnectorType.OpenAi)}', '{nameof(AiConnectorType.Ollama)}', '{nameof(AiConnectorType.AzureOpenAi)}' and '{nameof(AiConnectorType.Google)}'");
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
            case AiConnectorType.Google:
                settings = new GoogleChatCompletionClientSettings(connectionString.GoogleSettings);
                return true;
        }

        return false;
    }
    
    internal virtual IToolCallState CreateToolCallState()
    {
        return new ToolCallState();
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

    public abstract AiError ParseError(BlittableJsonReaderObject content, HttpResponseMessage response);

    public virtual string GetRefusal(BlittableJsonReaderObject choice0, BlittableJsonReaderObject message)
    {
        _ = choice0.TryGet(ChatCompletionClient.Constants.ResponseFields.Refusal, out string refusal)
            || message.TryGet(ChatCompletionClient.Constants.ResponseFields.Refusal, out refusal);

        return refusal;
    }

    public virtual ValueTask<BlittableJsonReaderObject> TryGetResponseContentAsync(JsonOperationContext context, Stream stream)
    {
        return context.ReadForMemoryAsync(stream, "response/object");
    }

    public virtual DynamicJsonValue GetAiAttachmentJson(AiAttachment attachment)
    {
        return attachment.Type switch
        {
            ChatCompletionClient.Constants.AttachmentsRequestFields.MediaTypeTextPlain => new DynamicJsonValue
            {
                [ChatCompletionClient.Constants.AttachmentsRequestFields.Type] = ChatCompletionClient.Constants.AttachmentsRequestFields.TypeText,
                [ChatCompletionClient.Constants.AttachmentsRequestFields.TypeText] = attachment.Data
            },
            ChatCompletionClient.Constants.AttachmentsRequestFields.MediaTypeApplicationPdf => new DynamicJsonValue
            {
                [ChatCompletionClient.Constants.AttachmentsRequestFields.Type] = ChatCompletionClient.Constants.AttachmentsRequestFields.File,
                [ChatCompletionClient.Constants.AttachmentsRequestFields.File] = new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.AttachmentsRequestFields.FileName] = attachment.Name,
                    [ChatCompletionClient.Constants.AttachmentsRequestFields.FileData] = "data:application/pdf;base64," + attachment.Data
                }
            },
            ChatCompletionClient.Constants.AttachmentsRequestFields.MediaTypeImageJpeg or
                ChatCompletionClient.Constants.AttachmentsRequestFields.MediaTypeImagePng or
                ChatCompletionClient.Constants.AttachmentsRequestFields.MediaTypeImageGif or
                ChatCompletionClient.Constants.AttachmentsRequestFields.MediaTypeImageWebp => new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.AttachmentsRequestFields.Type] = ChatCompletionClient.Constants.AttachmentsRequestFields.ImageUrl,
                    [ChatCompletionClient.Constants.AttachmentsRequestFields.ImageUrl] = new DynamicJsonValue
                    {
                        [ChatCompletionClient.Constants.AttachmentsRequestFields.Url] = "data:" + attachment.Type + ";base64," + attachment.Data
                    }
                },
            _ => throw new InvalidOperationException($"Attachment '{attachment.Name}' has unknown type: {attachment.Type}")
        };
    }
}

public class AiError
{
    public string Message { get; set; }
    public ErrorType ErrorType { get; set; }
    public TimeSpan? RetryAfter { get; set; } = null;
}

public enum ErrorType
{
    Unknown,
    InsufficientQuota,
    TooManyTokens,
    TooManyRequests,
    Other429,
    RefusedToAnswer,
}
