using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using Canonical = Raven.Server.Documents.AI.ChatCompletionClient.Constants;

namespace Raven.Server.Documents.AI.Settings;

// The OpenAI Responses API (/responses), used stateless (store: false): the whole conversation is sent on every call.
// Stored messages keep the canonical (Chat Completions) shape and are translated on every send, so a connection string
// can move back to Chat Completions and replay the same conversation.
internal abstract partial class AbstractOpenAiResponsesProvider : AbstractChatCompletionProvider
{
    protected AbstractOpenAiResponsesProvider(IAiSettings settings)
        : base(settings)
    {
    }

    // Same meaning as AbstractOpenAiCompatibleChatCompletionProvider.SupportStrictTools
    public virtual bool SupportStrictTools => true;

    // Same meaning as AbstractOpenAiCompatibleChatCompletionProvider.SupportsToolChoiceNone
    public virtual bool SupportsToolChoiceNone => true;

    public override string GetRelativeCompletionUri() => "responses";

    public override string GetRelativeModelsUri() => "models";

    public override void AddHeaders(HttpRequestMessage request)
    {
    }

    public override void AddAuthentication(HttpRequestMessage request) => OpenAiShared.AddBearerAuthentication(request, ApiKey);

    public override TimeSpan? GetRetryAfter(HttpResponseMessage response, AiError error) => OpenAiShared.GetRetryAfter(response, error);

    public override string GetRequestId(HttpResponseHeaders headers) => OpenAiShared.GetRequestId(headers);

    // Vendor fields such as temperature and reasoning; called last, after every other field is in the body.
    protected virtual void HandleCompletionRequestPayload(DynamicJsonValue body)
    {
    }

    // Temperature, and reasoning.effort when set (not set: "reasoning" is not sent, so the provider default applies).
    protected static void AddTemperatureAndReasoning(DynamicJsonValue body, OpenAiBaseSettings settings)
    {
        if (settings.Temperature.HasValue)
            body[Wire.Temperature] = settings.Temperature.Value;

        if (string.IsNullOrWhiteSpace(settings.ReasoningEffort) == false)
            body[Wire.Reasoning] = new DynamicJsonValue { [Wire.Effort] = OpenAiShared.NormalizeReasoningEffort(settings.ReasoningEffort.Trim()) };
    }

    public override DynamicJsonValue BuildTool(JsonOperationContext ctx, string name, string description, string parametersSchema)
    {
        var tool = new DynamicJsonValue
        {
            [Wire.Type] = Wire.TypeFunction,
            [Wire.Name] = name,
            [Wire.Description] = description,
            [Wire.Parameters] = ctx.Sync.ReadForMemory(parametersSchema, "params/schema")
        };

        if (SupportStrictTools)
            tool[Wire.Strict] = true;

        return tool;
    }

    protected override void WritePayloadCore(AsyncBlittableJsonTextWriter writer, JsonOperationContext ctx, AiChatRequest request, bool streaming)
    {
        var turns = NormalizeMessages(ctx, request);

        var body = new DynamicJsonValue
        {
            [Wire.Model] = RequestModel
        };

        if (turns.System != null)
            body[Wire.Instructions] = turns.System;

        body[Wire.Input] = turns.Messages;

        if (request.Tools?.Count > 0 && (request.UseTools || SupportsToolChoiceNone))
        {
            body[Wire.Tools] = request.Tools;

            if (request.UseTools == false)
                body[Wire.ToolChoice] = Wire.ToolChoiceNone;
        }

        if (request.Schema != null)
            body[Wire.Text] = new DynamicJsonValue { [Wire.Format] = BuildTextFormat(request) };

        if (request.PromptCacheKey != null && ShouldCachePrompt(request))
            body[Wire.PromptCacheKey] = request.PromptCacheKey;

        if (streaming)
            body[Wire.Stream] = true;

        body[Wire.Store] = false;

        HandleCompletionRequestPayload(body);

        ctx.Write(writer, body);
    }

    // The schema is {name, strict, schema}, as Chat Completions sends it inside response_format.json_schema;
    // Responses takes the same properties directly in text.format, next to "type".
    private static DynamicJsonValue BuildTextFormat(AiChatRequest request)
    {
        var schema = request.ParsedJson.GetOrParseSchema(request.Schema);

        var format = new DynamicJsonValue
        {
            [Wire.Type] = Wire.TypeJsonSchema
        };

        foreach (var property in schema.GetPropertyNames())
        {
            if (schema.TryGetMember(property, out var value))
                format[property] = value;
        }

        return format;
    }

    protected override ProviderMessages NormalizeMessages(JsonOperationContext ctx, AiChatRequest request)
    {
        var instructions = new StringBuilder();
        var input = new DynamicJsonArray();

        foreach (var message in request.Messages)
        {
            if (message.TryGet(Canonical.ResponseFields.Role, out string role) == false)
                continue;

            switch (role)
            {
                case Canonical.RequestFields.RoleSystemValue:
                    AppendInstructions(instructions, message);
                    break;
                case Canonical.RequestFields.RoleToolValue:
                    input.Add(ToFunctionCallOutput(message));
                    break;
                case Canonical.RequestFields.RoleAssistantValue:
                    AddAssistantItems(input, message);
                    break;
                default:
                    input.Add(ToInputMessage(role, message));
                    break;
            }
        }

        if (request.Attachments is { Count: > 0 })
            input.Add(BuildResponsesAttachmentsMessage(request.Attachments));

        return new ProviderMessages
        {
            Messages = input,
            System = instructions.Length > 0 ? instructions.ToString() : null
        };
    }

    private static void AppendInstructions(StringBuilder instructions, BlittableJsonReaderObject message)
    {
        message.TryGet(Canonical.ResponseFields.Content, out object content);
        var text = ContentAsText(content);
        if (string.IsNullOrEmpty(text))
            return;

        if (instructions.Length > 0)
            instructions.Append("\n\n");

        instructions.Append(text);
    }

    private static DynamicJsonValue ToFunctionCallOutput(BlittableJsonReaderObject message)
    {
        message.TryGet(Canonical.ResponseFields.ToolCallId, out string callId);
        message.TryGet(Canonical.ResponseFields.Content, out object content);

        return new DynamicJsonValue
        {
            [Wire.Type] = Wire.TypeFunctionCallOutput,
            [Wire.CallId] = callId,
            [Wire.Output] = ContentAsText(content) ?? string.Empty
        };
    }

    // An assistant turn becomes a text message (when it has text) and then one function_call item per tool call.
    // The function_call item id (fc_...) is not stored and not sent: with store=false there is nothing for it to refer to.
    private static void AddAssistantItems(DynamicJsonArray input, BlittableJsonReaderObject message)
    {
        message.TryGet(Canonical.ResponseFields.Content, out object content);
        var text = ContentAsText(content);
        if (string.IsNullOrEmpty(text) == false)
        {
            input.Add(new DynamicJsonValue
            {
                [Wire.Type] = Wire.TypeMessage,
                [Wire.Role] = Canonical.RequestFields.RoleAssistantValue,
                [Wire.Content] = new DynamicJsonArray
                {
                    new DynamicJsonValue
                    {
                        [Wire.Type] = Wire.TypeOutputText,
                        [Wire.Text] = text
                    }
                }
            });
        }

        if (message.TryGet(Canonical.ResponseFields.ToolCalls, out BlittableJsonReaderArray toolCalls) == false || toolCalls == null)
            return;

        foreach (var item in toolCalls)
        {
            if (item is not BlittableJsonReaderObject call ||
                call.TryGet(Canonical.ResponseFields.Id, out string id) == false ||
                call.TryGet(Canonical.ResponseFields.Function, out BlittableJsonReaderObject function) == false || function == null ||
                function.TryGet(Canonical.ResponseFields.Name, out string name) == false)
                continue;

            function.TryGet(Canonical.ResponseFields.Arguments, out string arguments);

            input.Add(new DynamicJsonValue
            {
                [Wire.Type] = Wire.TypeFunctionCall,
                [Wire.CallId] = id,
                [Wire.Name] = name,
                [Wire.Arguments] = arguments ?? "{}"
            });
        }
    }

    private DynamicJsonValue ToInputMessage(string role, BlittableJsonReaderObject message)
    {
        message.TryGet(Canonical.ResponseFields.Content, out object content);

        return new DynamicJsonValue
        {
            [Wire.Role] = role,
            [Wire.Content] = content is BlittableJsonReaderArray parts
                ? ToInputParts(parts)
                : ContentAsText(content) ?? string.Empty
        };
    }

    // Chat Completions content parts (text, image_url, file) as Responses input parts.
    private static DynamicJsonArray ToInputParts(BlittableJsonReaderArray parts)
    {
        var result = new DynamicJsonArray();
        foreach (var item in parts)
        {
            if (item is not BlittableJsonReaderObject part || part.TryGet(Canonical.AttachmentsRequestFields.Type, out string type) == false)
                continue;

            switch (type)
            {
                case Canonical.AttachmentsRequestFields.TypeText:
                    part.TryGet(Canonical.AttachmentsRequestFields.TypeText, out string text);
                    result.Add(InputText(text));
                    break;

                case Canonical.AttachmentsRequestFields.ImageUrl:
                    part.TryGet(Canonical.AttachmentsRequestFields.ImageUrl, out BlittableJsonReaderObject imageUrl);
                    string url = null;
                    imageUrl?.TryGet(Canonical.AttachmentsRequestFields.Url, out url);
                    result.Add(new DynamicJsonValue
                    {
                        [Wire.Type] = Wire.TypeInputImage,
                        [Wire.ImageUrl] = url
                    });
                    break;

                case Canonical.AttachmentsRequestFields.File:
                    part.TryGet(Canonical.AttachmentsRequestFields.File, out BlittableJsonReaderObject file);
                    string fileName = null, fileData = null;
                    file?.TryGet(Canonical.AttachmentsRequestFields.FileName, out fileName);
                    file?.TryGet(Canonical.AttachmentsRequestFields.FileData, out fileData);
                    result.Add(new DynamicJsonValue
                    {
                        [Wire.Type] = Wire.TypeInputFile,
                        [Wire.FileName] = fileName,
                        [Wire.FileData] = fileData
                    });
                    break;

                default:
                    throw new InvalidOperationException($"Message content part has unknown type: {type}");
            }
        }

        return result;
    }

    // Stored content as the text the model gets: a structured answer (a JSON object) is sent as its JSON text,
    // as Chat Completions does; text parts are joined.
    private static string ContentAsText(object content)
    {
        switch (content)
        {
            case null:
                return null;

            case BlittableJsonReaderArray parts:
                var text = new StringBuilder();
                foreach (var item in parts)
                {
                    if (item is BlittableJsonReaderObject part && part.TryGet(Canonical.AttachmentsRequestFields.TypeText, out string partText))
                        text.Append(partText);
                }
                return text.ToString();

            default:
                return content.ToString();
        }
    }

    // Same logic as BuildAttachmentsMessage, but the note for an attachment that wasn't found is an input_text part.
    private DynamicJsonValue BuildResponsesAttachmentsMessage(List<AiAttachment> attachments)
    {
        var content = new DynamicJsonArray();
        foreach (var attachment in attachments)
        {
            if (attachment.Source == AiAttachmentSource.NotFound)
            {
                content.Add(InputText($"File '{attachment.Name}' (of type '{attachment.Type}') could not be loaded: attachment not found"));
                continue;
            }

            content.Add(GetAiAttachmentJson(attachment));
        }

        return new DynamicJsonValue
        {
            [Wire.Role] = Canonical.RequestFields.RoleUserValue,
            [Wire.Content] = content
        };
    }

    public override DynamicJsonValue GetAiAttachmentJson(AiAttachment attachment)
    {
        return attachment.Type switch
        {
            Canonical.AttachmentsRequestFields.MediaTypeTextPlain => InputText(attachment.Data),
            Canonical.AttachmentsRequestFields.MediaTypeApplicationPdf => new DynamicJsonValue
            {
                [Wire.Type] = Wire.TypeInputFile,
                [Wire.FileName] = attachment.Name,
                [Wire.FileData] = "data:application/pdf;base64," + attachment.Data
            },
            Canonical.AttachmentsRequestFields.MediaTypeImageJpeg or
                Canonical.AttachmentsRequestFields.MediaTypeImagePng or
                Canonical.AttachmentsRequestFields.MediaTypeImageGif or
                Canonical.AttachmentsRequestFields.MediaTypeImageWebp => new DynamicJsonValue
                {
                    [Wire.Type] = Wire.TypeInputImage,
                    [Wire.ImageUrl] = "data:" + attachment.Type + ";base64," + attachment.Data
                },
            _ => throw new InvalidOperationException($"Attachment '{attachment.Name}' has unknown type: {attachment.Type}")
        };
    }

    private static DynamicJsonValue InputText(string text) => new()
    {
        [Wire.Type] = Wire.TypeInputText,
        [Wire.Text] = text
    };

    public override AiResponse ParseResponse(JsonOperationContext ctx, HttpResponseMessage response, BlittableJsonReaderObject content, AiUsage usage, bool structuredOutput)
    {
        // A refusal or a cut still uses tokens - account for them before throwing.
        content.TryGet(Wire.Usage, out BlittableJsonReaderObject usageJson);
        UpdateUsage(usageJson, usage);

        var requestId = GetRequestId(response.Headers);

        content.TryGet(Wire.Status, out string status);
        if (status == Wire.StatusFailed)
            throw UnexpectedResponseException.Create($"The model response failed: {ErrorMessage(content)}", response, content, requestId);

        var incompleteReason = status == Wire.StatusIncomplete ? IncompleteReason(content) : null;

        var text = new StringBuilder();
        StringBuilder refusal = null;
        List<AiToolCall> toolCalls = null;

        if (content.TryGet(Wire.Output, out BlittableJsonReaderArray output) && output != null)
        {
            foreach (var outputItem in output)
            {
                if (outputItem is not BlittableJsonReaderObject item || item.TryGet(Wire.Type, out string type) == false)
                    continue;

                switch (type)
                {
                    case Wire.TypeMessage:
                        AppendMessageParts(item, text, ref refusal);
                        break;

                    case Wire.TypeFunctionCall:
                        if (item.TryGet(Wire.CallId, out string callId) == false || item.TryGet(Wire.Name, out string name) == false)
                            throw UnexpectedResponseException.Create("Invalid function call: " + item, response, content, requestId);

                        item.TryGet(Wire.Arguments, out string arguments);
                        (toolCalls ??= new List<AiToolCall>()).Add(new AiToolCall(callId, name, arguments ?? "{}"));
                        break;

                    // reasoning items are not the answer
                }
            }
        }

        if (refusal is { Length: > 0 })
            RefusedToAnswerException.Throw(refusal.ToString(), content.ToString(), incompleteReason ?? status, requestId);

        var providerRefusal = GetProviderRefusal(content);
        if (providerRefusal != null)
            RefusedToAnswerException.Throw(providerRefusal, content.ToString(), incompleteReason ?? status, requestId);

        if (incompleteReason == Wire.ReasonContentFilter)
            RefusedToAnswerException.Throw(ContentFilterRefusal, content.ToString(), incompleteReason, requestId);

        // Partial plain text is still an answer; a cut structured object or a cut tool call is not.
        if (incompleteReason == Wire.ReasonMaxOutputTokens && (toolCalls != null || structuredOutput || text.Length == 0))
            throw Truncated(requestId, content);

        if (toolCalls != null)
        {
            return new AiResponse(AiResponseType.Tool)
            {
                ToolCalls = toolCalls,
                Message = CreateToolCallsMessage(ctx, ToToolCallsArray(toolCalls), "responses/tool-message")
            };
        }

        if (text.Length == 0)
            throw UnexpectedResponseException.Create("No response content", response, content, requestId);

        var answer = text.ToString();
        object result = structuredOutput
            ? ReadStructuredOutput(ctx, answer, $"status='{status}'", response, content)
            : answer;

        return new AiResponse(AiResponseType.Result)
        {
            Result = result,
            Message = CreateAssistantMessage(ctx, result, "responses/result-message")
        };
    }

    private static void AppendMessageParts(BlittableJsonReaderObject message, StringBuilder text, ref StringBuilder refusal)
    {
        if (message.TryGet(Wire.Content, out BlittableJsonReaderArray parts) == false || parts == null)
            return;

        foreach (var item in parts)
        {
            if (item is not BlittableJsonReaderObject part || part.TryGet(Wire.Type, out string type) == false)
                continue;

            switch (type)
            {
                case Wire.TypeOutputText:
                    if (part.TryGet(Wire.Text, out string partText))
                        text.Append(partText);
                    break;

                case Wire.TypeRefusal:
                    if (part.TryGet(Wire.TypeRefusal, out string refusalText))
                        (refusal ??= new StringBuilder()).Append(refusalText);
                    break;
            }
        }
    }

    private const string ContentFilterRefusal = "Response blocked due to content policy";

    // The value written as the body's "model".
    protected virtual string RequestModel => Model;

    // null = not refused. Called with the response object: the non-streaming body, or the "response" of a streamed
    // response.created / response.in_progress / response.completed / response.incomplete event.
    protected virtual string GetProviderRefusal(BlittableJsonReaderObject response) => null;

    private static string IncompleteReason(BlittableJsonReaderObject response)
    {
        if (response.TryGet(Wire.IncompleteDetails, out BlittableJsonReaderObject details) && details != null &&
            details.TryGet(Wire.Reason, out string reason))
            return reason;

        return null;
    }

    private static string ErrorMessage(BlittableJsonReaderObject response)
    {
        if (response.TryGet(Wire.Error, out BlittableJsonReaderObject error) && error != null)
        {
            error.TryGet(Wire.Code, out string code);
            error.TryGet(Wire.Message, out string message);
            return $"(code: '{code ?? "unknown"}') {message}";
        }

        return "no error details";
    }

    private static TooManyTokensException Truncated(string requestId, BlittableJsonReaderObject content) =>
        new($"The model response was truncated (incomplete_details.reason='{Wire.ReasonMaxOutputTokens}') before producing a complete answer." +
            (content == null ? null : $" Response content: {content}"))
        {
            RequestId = requestId
        };

    private static void UpdateUsage(BlittableJsonReaderObject usageJson, AiUsage usage)
    {
        if (usageJson == null)
            return;

        usageJson.TryGet(Wire.InputTokens, out long inputTokens);
        usageJson.TryGet(Wire.OutputTokens, out long outputTokens);
        if (usageJson.TryGet(Wire.TotalTokens, out long totalTokens) == false)
            totalTokens = inputTokens + outputTokens;

        long cachedTokens = 0;
        if (usageJson.TryGet(Wire.InputTokensDetails, out BlittableJsonReaderObject inputDetails) && inputDetails != null)
            inputDetails.TryGet(Wire.CachedTokens, out cachedTokens);

        long reasoningTokens = 0;
        if (usageJson.TryGet(Wire.OutputTokensDetails, out BlittableJsonReaderObject outputDetails) && outputDetails != null)
            outputDetails.TryGet(Wire.ReasoningTokens, out reasoningTokens);

        usage.PromptTokens += inputTokens;
        usage.CompletionTokens += outputTokens;
        usage.TotalTokens += totalTokens;
        usage.CachedTokens += cachedTokens;
        usage.ReasoningTokens += reasoningTokens;
    }

    protected static class Wire
    {
        // request
        public const string Model = "model";
        public const string Instructions = "instructions";
        public const string Input = "input";
        public const string Tools = "tools";
        public const string ToolChoice = "tool_choice";
        public const string ToolChoiceNone = "none";
        public const string Text = "text";
        public const string Format = "format";
        public const string TypeJsonSchema = "json_schema";
        public const string PromptCacheKey = "prompt_cache_key";
        public const string Stream = "stream";
        public const string Store = "store";
        public const string Temperature = "temperature";
        public const string Reasoning = "reasoning";
        public const string Effort = "effort";
        public const string Think = "think";

        // tools
        public const string TypeFunction = "function";
        public const string Description = "description";
        public const string Parameters = "parameters";
        public const string Strict = "strict";

        // input and output items
        public const string Type = "type";
        public const string Id = "id";
        public const string Role = "role";
        public const string Content = "content";
        public const string Name = "name";
        public const string Arguments = "arguments";
        public const string CallId = "call_id";
        public const string Output = "output";
        public const string TypeMessage = "message";
        public const string TypeFunctionCall = "function_call";
        public const string TypeFunctionCallOutput = "function_call_output";
        public const string TypeOutputText = "output_text";
        public const string TypeRefusal = "refusal";
        public const string TypeInputText = "input_text";
        public const string TypeInputImage = "input_image";
        public const string TypeInputFile = "input_file";
        public const string ImageUrl = "image_url";
        public const string FileName = "filename";
        public const string FileData = "file_data";

        // response
        public const string Status = "status";
        public const string StatusCompleted = "completed";
        public const string StatusIncomplete = "incomplete";
        public const string StatusFailed = "failed";
        public const string IncompleteDetails = "incomplete_details";
        public const string Reason = "reason";
        public const string ReasonMaxOutputTokens = "max_output_tokens";
        public const string ReasonContentFilter = "content_filter";
        public const string Error = "error";
        public const string Code = "code";
        public const string Message = "message";

        // usage
        public const string Usage = "usage";
        public const string InputTokens = "input_tokens";
        public const string OutputTokens = "output_tokens";
        public const string TotalTokens = "total_tokens";
        public const string InputTokensDetails = "input_tokens_details";
        public const string CachedTokens = "cached_tokens";
        public const string OutputTokensDetails = "output_tokens_details";
        public const string ReasoningTokens = "reasoning_tokens";

        // streaming events and their fields
        public const string Response = "response";
        public const string Item = "item";
        public const string ItemId = "item_id";
        public const string Delta = "delta";
        public const string EventCreated = "response.created";
        public const string EventInProgress = "response.in_progress";
        public const string EventOutputItemAdded = "response.output_item.added";
        public const string EventOutputItemDone = "response.output_item.done";
        public const string EventOutputTextDelta = "response.output_text.delta";
        public const string EventRefusalDelta = "response.refusal.delta";
        public const string EventFunctionCallArgumentsDelta = "response.function_call_arguments.delta";
        public const string EventCompleted = "response.completed";
        public const string EventIncomplete = "response.incomplete";
        public const string EventFailed = "response.failed";
        public const string EventError = "error";
    }
}
