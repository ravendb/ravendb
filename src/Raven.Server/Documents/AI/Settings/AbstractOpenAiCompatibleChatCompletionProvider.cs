using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.Json;
using Sparrow;
using Sparrow.Exceptions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.AI.Settings;

internal abstract partial class AbstractOpenAiCompatibleChatCompletionProvider : AbstractChatCompletionProvider
{
    protected AbstractOpenAiCompatibleChatCompletionProvider(IAiSettings settings)
        : base(settings)
    {
    }

    public virtual bool SupportStrictTools => true;

    public virtual bool SupportsToolChoiceNone => true;

    public override void AddHeaders(HttpRequestMessage request)
    {
    }

    public override string GetRelativeCompletionUri() => "chat/completions";

    public override string GetRelativeModelsUri() => "models";

    public abstract void HandleCompletionRequestPayload(AsyncBlittableJsonTextWriter writer);

    protected static class Wire
    {
        public static class RequestFields
        {
            public const string Think = "think";
            public const string Temperature = "temperature";
            public const string ReasoningEffort = "reasoning_effort";
            public const string Seed = "seed";
            public const string ReasoningEffortNoneValue = "none";
            public const string ToolTypeFunction = "function";
            public const string Parameters = "parameters";
            public const string ToolChoiceNone = "none";
        }

        public static class Headers
        {
            public const string OpenAiOrganization = "OpenAI-Organization";
            public const string OpenAiProject = "OpenAI-Project";
        }
    }

    public string GetRefusal(BlittableJsonReaderObject choice0, BlittableJsonReaderObject message, bool streaming = false)
        => GetRefusal(choice0, message, streaming, out _);

    // OpenAI's default: an explicit `refusal` field on the message (non-streaming) or on the delta
    // (streaming - GetRefusal gets the delta here as the "message"). Providers whose refusal
    // shape differs (Azure, Google) override this.
    //
    // isCompleteMessage tells a streaming caller how to accumulate the result: OpenAI streams the refusal
    // as text fragments on the delta that must be concatenated (false), whereas Azure/Google derive a full
    // message from finish_reason/content_filter_results per chunk that must NOT be concatenated (true).
    public virtual string GetRefusal(BlittableJsonReaderObject choice0, BlittableJsonReaderObject message, bool streaming, out bool isCompleteMessage)
    {
        isCompleteMessage = false;

        _ = choice0.TryGet(ChatCompletionClient.Constants.ResponseFields.Refusal, out string refusal)
            || (message != null && message.TryGet(ChatCompletionClient.Constants.ResponseFields.Refusal, out refusal));

        if (string.IsNullOrEmpty(refusal) == false)
            return refusal;

        if (string.Equals(GetFinishReason(choice0), ChatCompletionClient.Constants.ResponseFields.FinishReasonContentFilter, StringComparison.OrdinalIgnoreCase))
        {
            isCompleteMessage = true;
            return "Response blocked due to content policy";
        }

        return null;
    }

    public virtual string GetFinishReason(BlittableJsonReaderObject choice0)
    {
        choice0.TryGet(ChatCompletionClient.Constants.ResponseFields.FinishReason, out string finishReason);
        return finishReason;
    }

    public override DynamicJsonValue GetAiAttachmentJson(AiAttachment attachment)
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

    public override void AddAuthentication(HttpRequestMessage request)
    {
        request.Headers.Authorization = string.IsNullOrEmpty(ApiKey)
            ? null
            : new AuthenticationHeaderValue(ChatCompletionClient.Constants.RequestFields.AuthorizationApiKeyProperty, ApiKey);
    }

    public override DynamicJsonValue BuildTool(JsonOperationContext ctx, string name, string description, string parametersSchema)
    {
        var tool = new DynamicJsonValue
        {
            [ChatCompletionClient.Constants.JsonSchemaFields.Type] = Wire.RequestFields.ToolTypeFunction,
            [ChatCompletionClient.Constants.ResponseFields.Function] = new DynamicJsonValue
            {
                [ChatCompletionClient.Constants.ResponseFields.Name] = name,
                [ChatCompletionClient.Constants.JsonSchemaFields.Description] = description,
                [Wire.RequestFields.Parameters] = ctx.Sync.ReadForMemory(parametersSchema, "params/schema")
            }
        };

        if (SupportStrictTools)
            tool[ChatCompletionClient.Constants.JsonSchemaFields.Strict] = true;

        return tool;
    }

    public override void WritePayload(AsyncBlittableJsonTextWriter writer, JsonOperationContext ctx, AiChatRequest request, bool streaming)
    {
        writer.WriteStartObject();

        writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.Model);
        writer.WriteString(Model);
        writer.WriteComma();

        List<LazyStringValue> filterProperties = [ctx.GetLazyString(ConversationDocument.DateProperty), ctx.GetLazyString(ConversationDocument.UsageProperty), ctx.GetLazyString(ConversationDocument.OutputSchemaProperty)];

        writer.WriteArray(ctx, ChatCompletionClient.Constants.RequestFields.Messages, WithAttachments(ctx, request.Messages, request.Attachments), (w, context, message) =>
        {
            w.WriteStartObject();
            w.WriteObjectWithFilter(message, filterProperties.Contains);
            w.WriteEndObject();
        });

        // Optional
        if (request.PreparedTools?.Count > 0 && (request.UseTools || SupportsToolChoiceNone))
        {
            writer.WriteComma();
            writer.WriteArray(ChatCompletionClient.Constants.RequestFields.Tools, request.PreparedTools);

            if (request.UseTools is false)
            {
                writer.WriteComma();
                writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.ToolChoice);
                writer.WriteString(Wire.RequestFields.ToolChoiceNone);
            }
        }

        if (request.Schema != null)
        {
            writer.WriteComma();
            writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.ResponseFormat);
            writer.WriteStartObject();
            writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.Type);
            writer.WriteString(ChatCompletionClient.Constants.RequestFields.JsonSchema);
            writer.WriteComma();
            writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.JsonSchema);
            writer.WriteObject(GetStructuredOutputSchemaAsBlittable(ctx, request.Schema));
            writer.WriteEndObject();
        }

        if (streaming)
        {
            writer.WriteComma();
            writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.Stream);
            writer.WriteBool(true);
            writer.WriteComma();
            writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.StreamOptions);
            writer.WriteStartObject();
            writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.IncludeUsage);
            writer.WriteBool(true);
            writer.WriteEndObject();
        }

        if (request.PromptCacheKey != null && EnablePromptCaching)
        {
            writer.WriteComma();
            writer.WritePropertyName(ChatCompletionClient.Constants.RequestFields.PromptCacheKey);
            writer.WriteString(request.PromptCacheKey);
        }

        HandleCompletionRequestPayload(writer);

        writer.WriteEndObject();
    }

    private static BlittableJsonReaderObject GetStructuredOutputSchemaAsBlittable(JsonOperationContext ctx, string schema)
    {
        using (var stream = RecyclableMemoryStreamFactory.GetRecyclableStream(Encoding.UTF8.GetBytes(schema)))
        {
            return ctx.Sync.ReadForMemory(stream, "json");
        }
    }

    private IEnumerable<BlittableJsonReaderObject> WithAttachments(JsonOperationContext context, IEnumerable<BlittableJsonReaderObject> messages, List<AiAttachment> attachments)
    {
        foreach (var message in messages)
        {
            if (message.TryGet(ChatCompletionClient.Constants.RequestFields.Content, out object content))
            {
                // we need to stringify the content before sending to the model
                if (content is BlittableJsonReaderObject blittableJson)
                {
                    // clone once, not to change the original, since we are going to persist it
                    var msg = message.CloneOnTheSameContext();
                    var modifications = msg.Modifications ??= new DynamicJsonValue(msg);
                    modifications[ChatCompletionClient.Constants.RequestFields.Content] = blittableJson.ToString();
                    // clone twice, so the changes will take effect
                    yield return msg.CloneOnTheSameContext();
                    continue;
                }
            }

            yield return message;
        }

        if (attachments is not null && attachments.Count > 0)
            yield return context.ReadObject(BuildAttachmentsMessage(attachments), "write-ai-attachments");
    }

    public override AiResponse ParseResponse(JsonOperationContext ctx, HttpResponseMessage response, BlittableJsonReaderObject content, AiUsage usage, bool structuredOutput)
    {
        if (content.TryGet(ChatCompletionClient.Constants.ResponseFields.Choices, out BlittableJsonReaderArray choices) == false || choices.Length == 0)
            throw UnexpectedResponseException.Create(message: "No choices in response", response, content, GetRequestId(response.Headers));

        var choice0 = (BlittableJsonReaderObject)choices[0];

        // Some providers (e.g. Gemini's OpenAI-compatible API) omit "message" entirely and carry the refusal
        // (e.g. finish_reason "content_filter: PROHIBITED_CONTENT") or the finish_reason on the bare choice,
        // so Message can legitimately be null at this point.
        choice0.TryGet(ChatCompletionClient.Constants.ResponseFields.Message, out BlittableJsonReaderObject message);

        // A refusal or a token-limit stop still reports the tokens it consumed - account for them before throwing.
        var hasUsage = content.TryGet(ChatCompletionClient.Constants.ResponseFields.Usage, out BlittableJsonReaderObject usageJson);
        if (hasUsage)
            usage.UpdateFrom(usageJson);

        // Same order as the streaming path: refusal, then length, then tool calls / content.
        var finishReason = GetFinishReason(choice0);
        var lengthTruncated = string.Equals(finishReason, ChatCompletionClient.Constants.ResponseFields.FinishReasonLength, StringComparison.OrdinalIgnoreCase);

        var refusal = GetRefusal(choice0, message);
        if (string.IsNullOrEmpty(refusal) == false)
            RefusedToAnswerException.Throw(refusal, content.ToString(), finishReason, GetRequestId(response.Headers));

        if (message == null)
        {
            // A token-limit stop that produced no message at all is still a cut, not a malformed response.
            if (lengthTruncated)
                throw Truncated(response, content);

            throw UnexpectedResponseException.Create(message: "No message property in choice", response, content, GetRequestId(response.Headers));
        }

        if (hasUsage == false)
            throw UnexpectedResponseException.Create(message: "No usage in response content", response, content, GetRequestId(response.Headers));

        if (message.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray calls) && calls.Length > 0)
        {
            // A token-limit cut inside the tool call (even a partial call) must not reach the agent loop.
            if (lengthTruncated)
                throw Truncated(response, content);

            var toolCalls = new List<AiToolCall>();
            foreach (BlittableJsonReaderObject call in calls)
            {
                if (call.TryGet(ChatCompletionClient.Constants.ResponseFields.Id, out string callId) is false ||
                    call.TryGet(ChatCompletionClient.Constants.ResponseFields.Function, out BlittableJsonReaderObject function) is false ||
                    function.TryGet(ChatCompletionClient.Constants.ResponseFields.Name, out string name) is false ||
                    function.TryGet(ChatCompletionClient.Constants.ResponseFields.Arguments, out string args) is false)
                    throw UnexpectedResponseException.Create(message: "Invalid function call: " + call, response, content, GetRequestId(response.Headers));
                toolCalls.Add(new AiToolCall(callId, name, args));
            }

            return new AiResponse(AiResponseType.Tool) { ToolCalls = toolCalls, Message = message };
        }

        var hasContent = message.TryGet(ChatCompletionClient.Constants.ResponseFields.Content, out LazyStringValue contentStr) && contentStr?.Length > 0;

        var result = structuredOutput
            ? GetStructuredContent(ctx, response, content, choice0, message, contentStr, hasContent, finishReason, lengthTruncated)
            : GetPlainTextContent(response, content, choice0, message, contentStr, hasContent, finishReason, lengthTruncated);

        message.Modifications ??= new DynamicJsonValue(message);
        message.Modifications[ChatCompletionClient.Constants.ResponseFields.Content] = result;

        return new AiResponse(AiResponseType.Result) { Result = result, Message = message };
    }

    private object GetStructuredContent(JsonOperationContext ctx, HttpResponseMessage response, BlittableJsonReaderObject responseContent,
        BlittableJsonReaderObject choice0, BlittableJsonReaderObject message, LazyStringValue content, bool hasContent, string finishReason, bool lengthTruncated)
    {
        // a truncated response is incomplete even if its content is valid JSON
        if (lengthTruncated)
            throw Truncated(response, responseContent);

        // content -> reasoning_content -> reasoning: reasoning is only a fallback when no 'content' was
        // produced at all (RavenDB-25681)
        if (hasContent == false && TryGetDeltaReasoning(message, out content) == false)
            throw NoUsableAnswer(response, responseContent, choice0, message, finishReason);

        try
        {
            return ctx.Sync.ReadForMemory(content, "ai/output");
        }
        catch (Exception e) when (e is InvalidDataException or InvalidStartOfObjectException or EndOfStreamException)
        {
            throw UnexpectedResponseException.Create(
                $"Structured output was requested but the model returned content that is not valid JSON (finish_reason='{finishReason}'). Content: {content}",
                response, responseContent, GetRequestId(response.Headers));
        }
    }

    private object GetPlainTextContent(HttpResponseMessage response, BlittableJsonReaderObject responseContent,
        BlittableJsonReaderObject choice0, BlittableJsonReaderObject message, LazyStringValue content, bool hasContent, string finishReason, bool lengthTruncated)
    {
        if (hasContent == false)
        {
            if (lengthTruncated)
                throw Truncated(response, responseContent);

            if (TryGetDeltaContent(message, out content) == false)
                throw NoUsableAnswer(response, responseContent, choice0, message, finishReason);
        }

        return content.ToString();
    }

    private Exception NoUsableAnswer(HttpResponseMessage response, BlittableJsonReaderObject responseContent,
        BlittableJsonReaderObject choice0, BlittableJsonReaderObject message, string finishReason)
    {
        var refusal = GetRefusal(choice0, message);
        if (string.IsNullOrEmpty(refusal) == false)
            RefusedToAnswerException.Throw(refusal, responseContent.ToString(), finishReason, GetRequestId(response.Headers));

        return UnexpectedResponseException.Create(message: "No response content", response, responseContent, GetRequestId(response.Headers));
    }

    private TooManyTokensException Truncated(HttpResponseMessage response, BlittableJsonReaderObject responseContent) =>
        new($"The model response was truncated (finish_reason='length') before producing a complete answer. Response content: {responseContent}")
        {
            RequestId = GetRequestId(response.Headers)
        };

    private static bool TryGetDeltaContent(BlittableJsonReaderObject delta, out LazyStringValue content)
    {
        // Try content, then reasoning_content, then reasoning (for LM Studio and other reasoning model compatibility)
        if (delta.TryGet(ChatCompletionClient.Constants.ResponseFields.Content, out content) && content?.Length > 0)
            return true;

        return TryGetDeltaReasoning(delta, out content);
    }

    // Whether the message carries something to return. Providers whose refusal detection is a heuristic over the
    // shape of the message (Google) use this so GetRefusal stays safe to call on any response.
    protected static bool HasContentOrToolCalls(BlittableJsonReaderObject message)
    {
        if (message == null)
            return false;

        return TryGetDeltaContent(message, out _)
               || (message.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray calls) && calls is { Length: > 0 });
    }

    private static bool TryGetDeltaReasoning(BlittableJsonReaderObject delta, out LazyStringValue reasoning)
    {
        if (delta.TryGet(ChatCompletionClient.Constants.ResponseFields.ReasoningContent, out reasoning) && reasoning?.Length > 0)
            return true;

        if (delta.TryGet(ChatCompletionClient.Constants.ResponseFields.Reasoning, out reasoning) && reasoning?.Length > 0)
            return true;

        reasoning = null;
        return false;
    }

    public override TimeSpan? GetRetryAfter(HttpResponseMessage response, AiError error)
    {
        var headers = response.Headers;

        // The Retry-After headers only make the error retryable; their value is not read.
        if (headers.Contains(ChatCompletionClient.Constants.Headers.RetryAfterMs) == false &&
            headers.Contains(ChatCompletionClient.Constants.Headers.RetryAfter) == false &&
            error.RetryAfter == null)
            return null;

        var retryAfter = error.RetryAfter ?? TimeSpan.Zero;

        if (headers.TryGetValues(ChatCompletionClient.Constants.Headers.XRateLimitResetTokens, out var resetTokensValues))
        {
            // TPM
            var retryAfterAsString = resetTokensValues.FirstOrDefault();
            if (ChatCompletionClient.TryParseResetTime(retryAfterAsString, out var retryAfterForTokens) == false)
                throw new FormatException($"Unrecognized rate-limit format: '{retryAfterAsString}'");

            retryAfter = retryAfterForTokens > retryAfter ? retryAfterForTokens : retryAfter;
        }

        if (headers.TryGetValues(ChatCompletionClient.Constants.Headers.XRateLimitResetRequests, out var resetRequestsValues))
        {
            // RPM
            var retryAfterAsString = resetRequestsValues.FirstOrDefault();
            if (ChatCompletionClient.TryParseResetTime(retryAfterAsString, out var retryAfterForReqs) == false)
                throw new FormatException($"Unrecognized rate-limit format: '{retryAfterAsString}'");

            retryAfter = retryAfterForReqs > retryAfter ? retryAfterForReqs : retryAfter;
        }

        return retryAfter;
    }

    public override string GetRequestId(HttpResponseHeaders headers)
    {
        if (headers.TryGetValues(ChatCompletionClient.Constants.Headers.XRequestId, out IEnumerable<string> values))
            return values.FirstOrDefault() ?? string.Empty;

        // Azure API Management uses a different header name
        if (headers.TryGetValues("apim-request-id", out values))
            return values.FirstOrDefault() ?? string.Empty;

        return string.Empty;
    }
}
