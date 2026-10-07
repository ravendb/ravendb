using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI.Settings;

/// <summary>
/// Adapts one AI provider's chat API to RavenDB's canonical (OpenAI-shaped) messages: writes the request, parses the
/// response and classifies errors. <see cref="ChatCompletionClient"/> does the HTTP and delegates everything
/// provider-specific here.
/// </summary>
internal abstract class AbstractChatCompletionProvider
{
    private readonly IAiSettings _settings;
    
    public string ApiKey => _settings.ApiKey;

    public string Model => _settings.Model;

    public Uri GetBaseEndpointUri() => _settings.GetBaseEndpointUri();

    protected AbstractChatCompletionProvider(IAiSettings settings)
    {
        _settings = settings;
    }

    public abstract void AddHeaders(HttpRequestMessage request);

    public abstract string GetRelativeCompletionUri();

    public abstract string GetRelativeModelsUri();

    public virtual bool EnablePromptCaching => true;
    
    internal static bool TryGetParameters(AiConnectionString connectionString, out AbstractChatCompletionProvider provider)
    {
        provider = null;

        switch (connectionString.ModelType)
        {
            case AiModelType.Chat:
                break;
            default:
                throw new InvalidOperationException(
                    $"Invalid provider settings for '{connectionString.Name}' with model type '{connectionString.ModelType}'. " +
                    $"Supported providers for '{nameof(connectionString.ModelType.Chat)}' model type are '{nameof(AiConnectorType.OpenAi)}', '{nameof(AiConnectorType.Ollama)}', '{nameof(AiConnectorType.AzureOpenAi)}', '{nameof(AiConnectorType.Google)}' and '{nameof(AiConnectorType.Anthropic)}'");
        }

        var connectorType = connectionString.GetActiveProvider();
        switch (connectorType)
        {
            case AiConnectorType.OpenAi:
                provider = new OpenAiChatCompletionProvider(connectionString.OpenAiSettings);
                return true;
            case AiConnectorType.AzureOpenAi:
                provider = new AzureOpenAiChatCompletionProvider(connectionString.AzureOpenAiSettings);
                return true;
            case AiConnectorType.Ollama:
                provider = new OllamaChatCompletionProvider(connectionString.OllamaSettings);
                return true;
            case AiConnectorType.Google:
                provider = new GoogleChatCompletionProvider(connectionString.GoogleSettings);
                return true;
            case AiConnectorType.Anthropic:
                provider = new AnthropicChatCompletionProvider(connectionString.AnthropicSettings);
                return true;
        }

        return false;
    }
    
    public abstract AiError ParseError(BlittableJsonReaderObject content, HttpResponseMessage response);

    public abstract void AddAuthentication(HttpRequestMessage request);

    public abstract DynamicJsonValue BuildTool(JsonOperationContext ctx, string name, string description, string parametersSchema);

    // 'request' is resolved by the client: internal messages filtered, tools in this provider's shape.
    public abstract void WritePayload(AsyncBlittableJsonTextWriter writer, JsonOperationContext ctx, AiChatRequest request, bool streaming);

    public abstract AiResponse ParseResponse(JsonOperationContext ctx, HttpResponseMessage response, BlittableJsonReaderObject content, AiUsage usage, bool structuredOutput);

    // A new state for one streamed response; ProcessStreamEvent and BuildStreamedResponse receive the instance created here.
    public abstract ChatStreamState CreateStreamState();

    public abstract StreamEventResult ProcessStreamEvent(JsonOperationContext ctx, BlittableJsonReaderObject sseEvent, ChatStreamState state, AiUsage usage);

    public abstract AiResponse BuildStreamedResponse(JsonOperationContext streamingCtx, ChatStreamState state, HttpResponseMessage response);

    public abstract TimeSpan? GetRetryAfter(HttpResponseMessage response, AiError error);

    public abstract string GetRequestId(HttpResponseHeaders headers);

    public virtual ValueTask<BlittableJsonReaderObject> TryGetResponseContentAsync(JsonOperationContext context, Stream stream)
    {
        return context.ReadForMemoryAsync(stream, "response/object");
    }

    // One attachment as a content part in this provider's shape.
    public abstract DynamicJsonValue GetAiAttachmentJson(AiAttachment attachment);

    // One user message holding every attachment; an attachment that wasn't found becomes a text note.
    protected DynamicJsonValue BuildAttachmentsMessage(List<AiAttachment> attachments)
    {
        var content = new DynamicJsonArray();
        foreach (var attachment in attachments)
        {
            if (attachment.Source == AiAttachmentSource.NotFound)
            {
                content.Add(new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.AttachmentsRequestFields.Type] = ChatCompletionClient.Constants.AttachmentsRequestFields.TypeText,
                    [ChatCompletionClient.Constants.AttachmentsRequestFields.TypeText] = $"File '{attachment.Name}' (of type '{attachment.Type}') could not be loaded: attachment not found"
                });
                continue;
            }

            content.Add(GetAiAttachmentJson(attachment));
        }

        return new DynamicJsonValue
        {
            [ChatCompletionClient.Constants.RequestFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleUserValue,
            [ChatCompletionClient.Constants.RequestFields.Content] = content
        };
    }

    // The assistant turn as RavenDB stores it in the conversation (canonical, OpenAI-shaped), whatever the provider.
    protected static BlittableJsonReaderObject CreateAssistantMessage(JsonOperationContext ctx, object content, string debugTag) =>
        ctx.ReadObject(new DynamicJsonValue
        {
            [ChatCompletionClient.Constants.ResponseFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleAssistantValue,
            [ChatCompletionClient.Constants.ResponseFields.Content] = content
        }, debugTag);

    protected static BlittableJsonReaderObject CreateToolCallsMessage(JsonOperationContext ctx, DynamicJsonArray toolCalls, string debugTag) =>
        ctx.ReadObject(new DynamicJsonValue
        {
            [ChatCompletionClient.Constants.ResponseFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleAssistantValue,
            [ChatCompletionClient.Constants.ResponseFields.Content] = null,
            [ChatCompletionClient.Constants.ResponseFields.ToolCalls] = toolCalls
        }, debugTag);

    protected static DynamicJsonArray ToToolCallsArray(List<AiToolCall> toolCalls)
    {
        var array = new DynamicJsonArray();
        foreach (var call in toolCalls)
        {
            array.Add(new DynamicJsonValue
            {
                [ChatCompletionClient.Constants.ResponseFields.Id] = call.Id,
                [ChatCompletionClient.Constants.ResponseFields.Type] = ChatCompletionClient.Constants.ResponseFields.Function,
                [ChatCompletionClient.Constants.ResponseFields.Function] = new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.ResponseFields.Name] = call.Name,
                    [ChatCompletionClient.Constants.ResponseFields.Arguments] = call.Arguments
                }
            });
        }

        return array;
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
