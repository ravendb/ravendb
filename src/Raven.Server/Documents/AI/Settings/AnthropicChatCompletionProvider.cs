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
using Sparrow.Exceptions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using Canonical = Raven.Server.Documents.AI.ChatCompletionClient.Constants;

namespace Raven.Server.Documents.AI.Settings;

internal sealed partial class AnthropicChatCompletionProvider : AbstractChatCompletionProvider
{
    private readonly AnthropicSettings _settings;

    public AnthropicChatCompletionProvider(AnthropicSettings settings)
        : base(settings)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    }

    private int MaxOutputTokens => _settings.MaxOutputTokens ?? AnthropicSettings.DefaultMaxOutputTokens;

    public override bool EnablePromptCaching => _settings.EnablePromptCache ?? true;

    public override string GetRelativeCompletionUri() => "messages";

    public override string GetRelativeModelsUri() => "models";

    public override void AddAuthentication(HttpRequestMessage request)
    {
        request.Headers.TryAddWithoutValidation(Wire.HeaderApiKey, ApiKey);
    }

    public override void AddHeaders(HttpRequestMessage request)
    {
        var apiVersion = string.IsNullOrWhiteSpace(_settings.ApiVersion) ? AnthropicSettings.DefaultApiVersion : _settings.ApiVersion;
        request.Headers.TryAddWithoutValidation(Wire.HeaderAnthropicVersion, apiVersion);
    }

    public override DynamicJsonValue BuildTool(JsonOperationContext ctx, string name, string description, string parametersSchema)
    {
        var inputSchema = ParseJsonObject(ctx, parametersSchema);

        return new DynamicJsonValue
        {
            [Wire.Name] = name,
            [Wire.Description] = description,
            [Wire.InputSchema] = BuildStrictToolRootSchema(inputSchema),
            [Wire.Strict] = true
        };
    }

    public override void WritePayload(AsyncBlittableJsonTextWriter writer, JsonOperationContext ctx, AiChatRequest request, bool streaming)
    {
        var body = new DynamicJsonValue
        {
            [Wire.Model] = Model,
            [Wire.MaxTokens] = MaxOutputTokens
        };

        if (streaming)
            body[Wire.Stream] = true;

        DynamicJsonValue outputConfig = null;

        AppendReasoning(body, ref outputConfig);

        var turns = NormalizeMessages(ctx, request.Messages, request.Attachments);

        if (turns.System != null)
            body[Wire.System] = turns.System;

        body[Wire.Messages] = turns.Messages;

        // Anthropic honours tool_choice "none", so the tools stay in the request even when they are disabled.
        if (request.Tools?.Count > 0)
        {
            body[Wire.Tools] = request.Tools;

            if (request.UseTools == false)
                body[Wire.ToolChoice] = new DynamicJsonValue { [Wire.Type] = Wire.ToolChoiceNone };
        }

        var format = BuildOutputFormat(ctx, request.Schema);
        if (format != null)
            (outputConfig ??= new DynamicJsonValue())[Wire.Format] = format;

        if (outputConfig != null)
            body[Wire.OutputConfig] = outputConfig;

        // Agent conversations carry a cache key; GenAI requests on Anthropic don't, so they aren't cached.
        if (request.PromptCacheKey != null && EnablePromptCaching)
            body[Wire.CacheControl] = new DynamicJsonValue { [Wire.Type] = Wire.CacheEphemeral };

        ctx.Write(writer, body);
    }

    protected override ProviderMessages NormalizeMessages(JsonOperationContext ctx, IEnumerable<BlittableJsonReaderObject> payloadMessages, List<AiAttachment> attachments)
    {
        var systemText = new StringBuilder();
        var messages = new DynamicJsonArray();
        DynamicJsonArray pendingToolResults = null;

        foreach (var message in payloadMessages)
        {
            if (message.TryGet(Canonical.ResponseFields.Role, out string role) == false)
                continue;

            if (role == Canonical.RequestFields.RoleSystemValue)
            {
                if (message.TryGet(Canonical.ResponseFields.Content, out string sysContent) && string.IsNullOrEmpty(sysContent) == false)
                {
                    if (systemText.Length > 0)
                        systemText.Append("\n\n");
                    systemText.Append(sysContent);
                }
                continue;
            }

            if (role == Canonical.RequestFields.RoleToolValue)
            {
                pendingToolResults ??= new DynamicJsonArray();
                message.TryGet(Canonical.ResponseFields.ToolCallId, out string toolUseId);
                message.TryGet(Canonical.ResponseFields.Content, out object toolContent);
                pendingToolResults.Add(new DynamicJsonValue
                {
                    [Wire.Type] = Wire.TypeToolResult,
                    [Wire.ToolUseId] = toolUseId,
                    [Wire.Content] = toolContent?.ToString() ?? string.Empty
                });
                continue;
            }

            FlushToolResults(messages, pendingToolResults);
            pendingToolResults = null;

            if (role == Canonical.RequestFields.RoleAssistantValue)
            {
                if (TryBuildAssistantTurn(ctx, message, out var assistantTurn))
                    messages.Add(assistantTurn);
                continue;
            }

            message.TryGet(Canonical.ResponseFields.Content, out object userContent);
            var userBlocks = new DynamicJsonArray();
            AppendContentText(userBlocks, userContent);

            if (userBlocks.Count == 0)
                continue;

            messages.Add(new DynamicJsonValue { [Wire.Role] = Wire.RoleUser, [Wire.Content] = userBlocks });
        }

        FlushToolResults(messages, pendingToolResults);

        AppendAttachments(messages, attachments);

        if (messages.Count == 0)
            throw new InvalidOperationException(
                "Cannot build an Anthropic request: every message was empty after normalization, leaving no content to send. " +
                "A turn whose content is an empty string (or whose parts are all empty) produces no content block, and Anthropic " +
                "rejects an empty text block, so such turns are dropped rather than padded.");

        return new ProviderMessages
        {
            Messages = messages,
            System = systemText.Length > 0 ? systemText.ToString() : null
        };
    }

    private void AppendReasoning(DynamicJsonValue body, ref DynamicJsonValue outputConfig)
    {
        if (string.IsNullOrWhiteSpace(_settings.ReasoningEffort))
            return;

        body[Wire.Thinking] = new DynamicJsonValue { [Wire.Type] = Wire.ThinkingAdaptive };
        (outputConfig ??= new DynamicJsonValue())[Wire.Effort] = EffortLevel();
    }

    // Anthropic's effort levels are lowercase and case-sensitive; any value goes out as given, lowercased, and the API validates it.
    private string EffortLevel() => _settings.ReasoningEffort.Trim().ToLowerInvariant();

    // Anthropic's strict mode requires `additionalProperties: false` on every object in a tool schema, and RavenDB's own
    // sub-agent schema generator does not emit it on the root. Only the root is repaired: the schemas RavenDB generates are either
    // flat (sub-agent parameters are primitives) or already closed at every level (the sample-object generator), and a
    // hand-written schema belongs to the agent author - if it declares something Anthropic rejects, Anthropic says so.
    //
    // What RavenDB generates for a sub-agent tool (GetSchemaForSubAgentTool), e.g. one string parameter:
    //   {"type":"object","properties":{"customerId":{"description":"The customer to look up","type":"string"}},"required":["customerId"]}
    // goes out to Anthropic as the same object with "additionalProperties": false appended at the root.
    // A schema that already states additionalProperties is sent as-is; a tool with no parameters gets
    //   {"type":"object","properties":{},"additionalProperties":false}
    private static object BuildStrictToolRootSchema(BlittableJsonReaderObject schema)
    {
        if (schema.Count == 0)
            return new DynamicJsonValue
            {
                [Canonical.JsonSchemaFields.Type] = Canonical.JsonSchemaFields.TypeObject,
                [Canonical.JsonSchemaFields.Properties] = new DynamicJsonValue(),
                [Canonical.JsonSchemaFields.AdditionalProperties] = false
            };

        if (schema.TryGetMember(Canonical.JsonSchemaFields.AdditionalProperties, out _))
            return schema;

        var root = new DynamicJsonValue();
        foreach (var property in schema.GetPropertyNames())
        {
            if (schema.TryGetMember(property, out var value))
                root[property] = value;
        }

        root[Canonical.JsonSchemaFields.AdditionalProperties] = false;
        return root;
    }

    // Consecutive tool results go back as one user turn.
    private static void FlushToolResults(DynamicJsonArray messages, DynamicJsonArray pendingToolResults)
    {
        if (pendingToolResults == null)
            return;

        messages.Add(new DynamicJsonValue { [Wire.Role] = Wire.RoleUser, [Wire.Content] = pendingToolResults });
    }

    private static bool TryBuildAssistantTurn(JsonOperationContext ctx, BlittableJsonReaderObject message, out DynamicJsonValue turn)
    {
        turn = null;

        var content = new DynamicJsonArray();

        message.TryGet(Canonical.ResponseFields.Content, out object textContent);
        AppendContentText(content, textContent);

        if (message.TryGet(Canonical.ResponseFields.ToolCalls, out BlittableJsonReaderArray toolCalls) && toolCalls != null)
        {
            foreach (BlittableJsonReaderObject call in toolCalls)
            {
                if (call.TryGet(Canonical.ResponseFields.Id, out string id) == false ||
                    call.TryGet(Canonical.ResponseFields.Function, out BlittableJsonReaderObject function) == false ||
                    function.TryGet(Canonical.ResponseFields.Name, out string name) == false)
                    continue;

                function.TryGet(Canonical.ResponseFields.Arguments, out string arguments);
                content.Add(new DynamicJsonValue
                {
                    [Wire.Type] = Wire.TypeToolUse,
                    [Wire.Id] = id,
                    [Wire.Name] = name,
                    [Wire.Input] = ParseJsonObject(ctx, arguments)
                });
            }
        }

        if (content.Count == 0)
            return false;

        turn = new DynamicJsonValue { [Wire.Role] = Wire.RoleAssistant, [Wire.Content] = content };
        return true;
    }

    private void AppendAttachments(DynamicJsonArray messages, List<AiAttachment> attachments)
    {
        if (attachments == null || attachments.Count == 0)
            return;

        messages.Add(BuildAttachmentsMessage(attachments));
    }

    public override DynamicJsonValue GetAiAttachmentJson(AiAttachment attachment)
    {
        switch (attachment.Type)
        {
            case Canonical.AttachmentsRequestFields.MediaTypeTextPlain:
                return TextBlock(attachment.Data);
            case Canonical.AttachmentsRequestFields.MediaTypeApplicationPdf:
                return new DynamicJsonValue
                {
                    [Wire.Type] = Wire.TypeDocument,
                    [Wire.Source] = Base64Source(Canonical.AttachmentsRequestFields.MediaTypeApplicationPdf, attachment.Data)
                };
            case Canonical.AttachmentsRequestFields.MediaTypeImageJpeg:
            case Canonical.AttachmentsRequestFields.MediaTypeImagePng:
            case Canonical.AttachmentsRequestFields.MediaTypeImageGif:
            case Canonical.AttachmentsRequestFields.MediaTypeImageWebp:
                return new DynamicJsonValue
                {
                    [Wire.Type] = Wire.TypeImage,
                    [Wire.Source] = Base64Source(attachment.Type, attachment.Data)
                };
            default:
                throw new InvalidOperationException($"Attachment '{attachment.Name}' has unknown type: {attachment.Type}");
        }
    }

    private static DynamicJsonValue BuildOutputFormat(JsonOperationContext ctx, string schema)
    {
        if (string.IsNullOrWhiteSpace(schema))
            return null;

        var wrapper = ParseJsonObject(ctx, schema);
        object innerSchema = wrapper != null && wrapper.TryGetMember(Wire.Schema, out var s) ? s : wrapper;

        return new DynamicJsonValue
        {
            [Wire.Type] = Wire.TypeJsonSchema,
            [Wire.Schema] = innerSchema
        };
    }

    public override AiResponse ParseResponse(JsonOperationContext ctx, HttpResponseMessage response, BlittableJsonReaderObject content, AiUsage usage, bool structuredOutput)
    {
        UpdateUsage(content, usage);

        content.TryGet(Wire.StopReason, out string stopReason);

        // Partial plain text is still an answer; a cut structured object or a cut tool call is not.
        var lengthTruncated = stopReason is Wire.StopReasonMaxTokens or Wire.StopReasonModelContextWindowExceeded;
        if (lengthTruncated && structuredOutput)
            throw new TooManyTokensException($"The model stopped because it ran out of room (stop_reason='{stopReason}'). Response: {content}") { RequestId = GetRequestId(response.Headers) };

        if (content.TryGet(Wire.Content, out BlittableJsonReaderArray contentArray) == false || contentArray == null)
            throw UnexpectedResponseException.Create("No content in response", response, content, GetRequestId(response.Headers));

        var text = new StringBuilder();
        List<AiToolCall> toolCalls = null;

        foreach (BlittableJsonReaderObject block in contentArray)
        {
            if (block.TryGet(Wire.Type, out string type) == false)
                continue;

            switch (type)
            {
                case Wire.TypeText:
                    if (block.TryGet(Wire.Text, out string blockText))
                        text.Append(blockText);
                    break;

                case Wire.TypeToolUse:
                    if (block.TryGet(Wire.Id, out string id) == false || block.TryGet(Wire.Name, out string name) == false)
                        throw UnexpectedResponseException.Create("Invalid function call: " + block, response, content, GetRequestId(response.Headers));
                    block.TryGet(Wire.Input, out BlittableJsonReaderObject input);
                    (toolCalls ??= new List<AiToolCall>()).Add(new AiToolCall(id, name, input?.ToString() ?? "{}"));
                    break;

                // thinking / redacted_thinking are intentionally NOT surfaced into answer content.
            }
        }

        if (stopReason == Wire.StopReasonRefusal)
            RefusedToAnswerException.Throw(RefusalText(text.ToString()), content.ToString(), stopReason, GetRequestId(response.Headers));

        if (lengthTruncated && (toolCalls != null || text.Length == 0))
            throw new TooManyTokensException($"The model stopped because it ran out of room (stop_reason='{stopReason}'). Response: {content}") { RequestId = GetRequestId(response.Headers) };

        if (toolCalls != null)
        {
            return new AiResponse(AiResponseType.Tool)
            {
                ToolCalls = toolCalls,
                Message = CreateToolCallsMessage(ctx, ToToolCallsArray(toolCalls), "anthropic/tool-message")
            };
        }

        if (text.Length == 0)
            throw UnexpectedResponseException.Create("No text content in response", response, content, GetRequestId(response.Headers));

        // Unstructured (no schema): the text IS the answer, returned as a string without parsing.
        var answer = text.ToString();
        object result = answer;
        if (structuredOutput)
        {
            try
            {
                result = ctx.Sync.ReadForMemory(answer, "ai/output");
            }
            catch (Exception e) when (e is InvalidDataException or InvalidStartOfObjectException or EndOfStreamException)
            {
                throw UnexpectedResponseException.Create(
                    $"Structured output was requested but the model returned content that is not valid JSON (stop_reason='{stopReason}'). Content: {text}",
                    response, content, GetRequestId(response.Headers));
            }
        }

        return new AiResponse(AiResponseType.Result)
        {
            Result = result,
            Message = CreateAssistantMessage(ctx, result, "anthropic/result-message")
        };
    }

    private static void UpdateUsage(BlittableJsonReaderObject content, AiUsage usage)
    {
        if (content.TryGet(Wire.Usage, out BlittableJsonReaderObject usageJson) == false || usageJson == null)
            return;

        usageJson.TryGet(Wire.InputTokens, out long inputTokens);
        usageJson.TryGet(Wire.OutputTokens, out long outputTokens);
        usageJson.TryGet(Wire.CacheReadInputTokens, out long cacheRead);
        usageJson.TryGet(Wire.CacheCreationInputTokens, out long cacheCreation);

        // Prompt tokens = fresh input + cache-read + cache-creation; cached = cache-read.
        UpdateUsage(usage, promptTokens: inputTokens + cacheRead + cacheCreation, completionTokens: outputTokens, cachedTokens: cacheRead,
            reasoningTokens: ThinkingTokens(usageJson));
    }

    private static void UpdateUsage(AiUsage usage, long promptTokens, long completionTokens, long cachedTokens, long reasoningTokens)
    {
        usage.PromptTokens += promptTokens;
        usage.CompletionTokens += completionTokens;
        usage.TotalTokens += promptTokens + completionTokens;
        usage.CachedTokens += cachedTokens;
        usage.ReasoningTokens += reasoningTokens;
    }

    private static long ThinkingTokens(BlittableJsonReaderObject usageJson) =>
        usageJson.TryGet(Wire.OutputTokensDetails, out BlittableJsonReaderObject details) && details != null &&
        details.TryGet(Wire.ThinkingTokens, out long thinkingTokens)
            ? thinkingTokens
            : 0;

    public override AiError ParseError(BlittableJsonReaderObject content, HttpResponseMessage response)
    {
        var (type, message) = ExtractError(content);

        // A 429 without a usable retry-after has no delay to honour - the monthly spend cap answers that way and keeps
        // failing until it resets - so it backs off like an exhausted quota instead of being retried at once.
        var errorType = (int)response.StatusCode == 429
            ? (TryParseRetryAfterHeaders(response.Headers, out _) ? ErrorType.TooManyRequests : ErrorType.InsufficientQuota)
            : IsInputOverflow(response, type, message) ? ErrorType.TooManyTokens
            : ErrorType.Unknown;

        return new AiError
        {
            ErrorType = errorType,
            Message = message
        };
    }

    private static bool IsInputOverflow(HttpResponseMessage response, string errorType, string message) =>
        (int)response.StatusCode == 400 &&
        errorType == Wire.ErrorInvalidRequest &&
        message?.StartsWith("prompt is too long", StringComparison.OrdinalIgnoreCase) == true;

    public override TimeSpan? GetRetryAfter(HttpResponseMessage response, AiError error)
    {
        // "prompt is too long" is a deterministic overflow; a stray Retry-After must not make it retryable.
        if (error.ErrorType == ErrorType.TooManyTokens)
            return null;

        return TryParseRetryAfterHeaders(response.Headers, out var fromHeader) ? fromHeader : null;
    }

    // Anthropic documents only the standard Retry-After header, in seconds; an HTTP date is accepted too.
    private static bool TryParseRetryAfterHeaders(HttpResponseHeaders headers, out TimeSpan retryAfter)
    {
        var standard = headers.RetryAfter;
        if (standard?.Delta is { } delta)
        {
            retryAfter = delta;
            return true;
        }

        if (standard?.Date is { } date)
        {
            var wait = date - DateTimeOffset.UtcNow;
            retryAfter = wait < TimeSpan.Zero ? TimeSpan.Zero : wait;
            return true;
        }

        retryAfter = default;
        return false;
    }

    public override string GetRequestId(HttpResponseHeaders headers)
    {
        if (headers.TryGetValues(Wire.HeaderRequestId, out var values))
            return values.FirstOrDefault() ?? string.Empty;

        return string.Empty;
    }

    private static (string Type, string Message) ExtractError(BlittableJsonReaderObject content)
    {
        if (content != null && content.TryGet(Wire.Error, out BlittableJsonReaderObject error) && error != null)
        {
            error.TryGet(Wire.Type, out string type);
            error.TryGet(Wire.Message, out string message);
            return (type, message);
        }

        return (null, null);
    }

    private static DynamicJsonValue TextBlock(string text) => new() { [Wire.Type] = Wire.TypeText, [Wire.Text] = text };

    private static void AppendContentText(DynamicJsonArray target, object content)
    {
        switch (content)
        {
            case null:
                return;

            case BlittableJsonReaderArray parts:
                foreach (var part in parts)
                {
                    if (part is BlittableJsonReaderObject partObj && partObj.TryGet(Canonical.AttachmentsRequestFields.TypeText, out string partText))
                    {
                        if (string.IsNullOrWhiteSpace(partText) == false)
                            target.Add(TextBlock(partText));
                    }
                    else if (part != null)
                    {
                        var s = part.ToString();
                        if (string.IsNullOrWhiteSpace(s) == false)
                            target.Add(TextBlock(s));
                    }
                }
                return;

            case BlittableJsonReaderObject obj:
                target.Add(TextBlock(obj.ToString()));
                return;

            default:
                var text = content.ToString();
                if (string.IsNullOrWhiteSpace(text) == false)
                    target.Add(TextBlock(text));
                return;
        }
    }

    // A refusal from Anthropic's classifier can come with no text at all.
    private static string RefusalText(string text) => string.IsNullOrWhiteSpace(text) ? "The model refused to answer and gave no reason" : text;

    private static DynamicJsonValue Base64Source(string mediaType, string data) => new()
    {
        [Wire.Type] = Wire.SourceBase64,
        [Wire.MediaType] = mediaType,
        [Wire.Data] = data
    };

    private static BlittableJsonReaderObject ParseJsonObject(JsonOperationContext ctx, string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return ctx.ReadObject(new DynamicJsonValue(), "empty/json");

        return ctx.Sync.ReadForMemory(json, "json");
    }

    private static class Wire
    {
        // request/response field names (Anthropic Messages API)
        public const string Model = "model";
        public const string MaxTokens = "max_tokens";
        public const string System = "system";
        public const string Messages = "messages";
        public const string Role = "role";
        public const string Content = "content";
        public const string Type = "type";
        public const string Text = "text";
        public const string Source = "source";
        public const string SourceBase64 = "base64";
        public const string MediaType = "media_type";
        public const string Data = "data";
        public const string Tools = "tools";
        public const string ToolChoice = "tool_choice";
        public const string ToolChoiceNone = "none";
        public const string Name = "name";
        public const string Description = "description";
        public const string InputSchema = "input_schema";
        public const string Strict = "strict";
        public const string Input = "input";
        public const string OutputConfig = "output_config";
        public const string CacheControl = "cache_control";
        public const string CacheEphemeral = "ephemeral";
        public const string Format = "format";
        public const string Effort = "effort";
        public const string Schema = "schema";
        public const string TypeJsonSchema = "json_schema";
        public const string Usage = "usage";
        public const string InputTokens = "input_tokens";
        public const string OutputTokens = "output_tokens";
        public const string OutputTokensDetails = "output_tokens_details";
        public const string ThinkingTokens = "thinking_tokens";
        public const string CacheReadInputTokens = "cache_read_input_tokens";
        public const string CacheCreationInputTokens = "cache_creation_input_tokens";
        public const string StopReason = "stop_reason";
        public const string StopReasonMaxTokens = "max_tokens";
        public const string StopReasonModelContextWindowExceeded = "model_context_window_exceeded";
        public const string StopReasonRefusal = "refusal";
        public const string Error = "error";
        public const string Message = "message";

        // Anthropic error object `type` values (same set on an HTTP error body and on an SSE error event).
        public const string ErrorInvalidRequest = "invalid_request_error";
        public const string ErrorAuthentication = "authentication_error";
        public const string ErrorPermission = "permission_error";
        public const string ErrorNotFound = "not_found_error";
        public const string ErrorRequestTooLarge = "request_too_large";
        public const string ErrorRateLimit = "rate_limit_error";
        public const string ErrorApi = "api_error";
        public const string ErrorOverloaded = "overloaded_error";

        // content-block types
        public const string TypeText = "text";
        public const string TypeToolUse = "tool_use";
        public const string TypeToolResult = "tool_result";
        public const string TypeImage = "image";
        public const string TypeDocument = "document";
        public const string ToolUseId = "tool_use_id";

        // streaming (SSE): event types, delta types, and their fields
        public const string Stream = "stream";
        public const string Index = "index";
        public const string ContentBlock = "content_block";
        public const string Delta = "delta";
        public const string PartialJson = "partial_json";
        public const string Thinking = "thinking";
        public const string ThinkingAdaptive = "adaptive";
        public const string EventMessageStart = "message_start";
        public const string EventContentBlockStart = "content_block_start";
        public const string EventContentBlockDelta = "content_block_delta";
        public const string EventMessageDelta = "message_delta";
        public const string EventMessageStop = "message_stop";
        public const string EventError = "error";
        public const string DeltaText = "text_delta";
        public const string DeltaInputJson = "input_json_delta";

        // message roles and tool-use ids
        public const string RoleUser = "user";
        public const string RoleAssistant = "assistant";
        public const string Id = "id";

        // headers
        public const string HeaderApiKey = "x-api-key";
        public const string HeaderAnthropicVersion = "anthropic-version";
        public const string HeaderRequestId = "request-id";
    }
}
