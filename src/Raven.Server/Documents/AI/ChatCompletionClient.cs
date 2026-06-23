using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.ServerSentEvents;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.IdentityModel.Tokens;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Raven.Server.Documents.AI;

public class ChatCompletionClient : IDisposable
{
    public static readonly string EmptySchema = GetSchemaFromSampleObject("{}");

    private readonly AbstractChatCompletionClientSettings _settings;
    private readonly HttpClientCacheKey _httpClientCacheKey;
    private readonly HttpClient _client;
    private readonly IMemoryContextPool _contextPool;

    public static readonly DocumentConventions ConventionsToUse = new DocumentConventions
    {
        SendApplicationIdentifier = DocumentConventions.DefaultForServer.SendApplicationIdentifier,
        MaxContextSizeToKeep = DocumentConventions.DefaultForServer.MaxContextSizeToKeep,
        HttpPooledConnectionLifetime = DocumentConventions.DefaultForServer.HttpPooledConnectionLifetime,
        DisposeCertificate = DocumentConventions.DefaultForServer.DisposeCertificate,
        DisableTopologyCache = DocumentConventions.DefaultForServer.DisableTopologyCache,
        UseHttpCompression = false
    };

    static ChatCompletionClient()
    {
        ConventionsToUse.Freeze();
    }

    public static ChatCompletionClient CreateChatCompletionClient(IMemoryContextPool contextPool, AiConnectionString connection)
    {
        if (AbstractChatCompletionClientSettings.TryGetParameters(connection, out var settings) == false)
        {
            var connectorType = connection.GetActiveProvider();
            throw new NotSupportedException($"The specified provider (\"{connectorType.ToString()}\") is not supported.");
        }

        return new ChatCompletionClient(contextPool, settings, ConventionsToUse);
    }

    internal ChatCompletionClient(IMemoryContextPool contextPool, AbstractChatCompletionClientSettings settings, DocumentConventions conventions = null)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));

        conventions ??= ConventionsToUse;

        var baseUri = settings.GetBaseEndpointUri();

        _httpClientCacheKey = HttpClientCacheKey.Create(conventions.UseHttpDecompression,
            conventions.HasExplicitlySetDecompressionUsage, conventions.HttpPooledConnectionLifetime,
            conventions.HttpPooledConnectionIdleTimeout, conventions.GlobalHttpClientTimeout,
            baseUri.ToString(), conventions.ConfigureHttpMessageHandler);

        _client = DefaultRavenHttpClientFactory.Instance.GetHttpClient(_httpClientCacheKey, handler => new HttpClient(handler)
        {
            BaseAddress = baseUri
        });

        _contextPool = contextPool;
    }

    public List<BlittableJsonReaderObject> GenerateTools(JsonOperationContext context, AiAgentConfiguration configuration, ConversationHandler handler)
    {
        var persistedAttachmentsNames = handler._persistedAttachmentsNames;
        if (persistedAttachmentsNames is { Count: > 0 })
        {
            configuration.Actions.Add(new AiAgentToolAction
            {
                Name = Constants.ToolNames.RetrieveAttachment,
                Description =
                    $"Retrieves one or more attachments by their names. Use this to re-read files. {Environment.NewLine} Available Attachments: {string.Join(", ", persistedAttachmentsNames)}",
                ParametersSampleObject = "{\"names\": [\"**ATTENTION IMPORTANT INSTRUCTION**: use **ONLY** the names from the tool description\"]}",
            });
        }

        List<BlittableJsonReaderObject> tools = [];
        foreach (var q in configuration.Queries ?? [])
        {
            if (q.ShouldAllowModelQueries() == false)
                continue;

            var paramsSchema = GetSchemaForTool(q.ParametersSchema, q.ParametersSampleObject);
            var tool = GetTool(context, q.Name, q.Description, paramsSchema);
            tools.Add(context.ReadObject(tool, "tool"));
        }

        foreach (var a in configuration.Actions ?? [])
        {
            string paramsSchema = GetSchemaForTool(a.ParametersSchema, a.ParametersSampleObject);
            var tool = GetTool(context, a.Name, a.Description, paramsSchema);
            tools.Add(context.ReadObject(tool, "tool"));
        }

        foreach (var subAgent in configuration.SubAgents ?? [])
        {
            var subAgentConfiguration = handler.GetAiAgentConfiguration(subAgent.Identifier);
            var parameters = handler.BuildSubAgentParameters(context, configuration, subAgentConfiguration);
            var paramsSchema = ConversationHandler.GetSchemaForSubAgentTool(context, parameters);
            var description = new StringBuilder(subAgent.Description).AppendLine();
            subAgentConfiguration.AppendCapabilities(description);
            var tool = GetTool(context, subAgent.Identifier, description.ToString(), paramsSchema);
            tools.Add(context.ReadObject(tool, "tool"));
        }

        return tools;
    }

    public DynamicJsonValue GetTool(JsonOperationContext context, string name, string description, string paramsSchema)
    {
        var tool = new DynamicJsonValue
        {
            [Constants.JsonSchemaFields.Type] = "function",
            [Constants.ResponseFields.Function] = new DynamicJsonValue
            {
                [Constants.ResponseFields.Name] = name,
                [Constants.JsonSchemaFields.Description] = description,
                ["parameters"] = context.Sync.ReadForMemory(paramsSchema, "params/schema")
            }
        };

        if (_settings.SupportStrictTools)
            tool[Constants.JsonSchemaFields.Strict] = true;

        return tool;
    }

    public async Task<AiResponse> StreamingCompleteAsync(JsonOperationContext streamingContext, IMemoryContextPool contextPool,
        string streamPropertyPath, HttpRequestMessage request,
        Func<Memory<byte>, Task> streamedPropertyCallback,
        AiUsage usage, AiDebugTrace trace, CancellationToken token)
    {
        AddDefaultHeaders(request);
        using var streamedPropertyBuffer = new JsonOperationContextBuffer<byte>(streamingContext);

        var parser = new SseStreamingJsonParser(streamingContext, streamPropertyPath);
        var alreadySeen = 0;
        parser.OnStringRead += (e) =>
        {
            // the `e` we get here is the _full_ string (including past chunks we already saw)
            // we want to read only the *new* parts, that we didn't see before
            alreadySeen += streamedPropertyBuffer.Append(alreadySeen, e);
        };

        using var response = await SendStreamingRequestAsync(request, token);
        if (response.IsSuccessStatusCode == false)
        {
            var responseContent = await GetResponseContentAsync(streamingContext, response, token);
            HandleUnsuccessfulResponse(response, responseContent);
            Debug.Assert(false, "we should never get here");
        }

        await using var responseStream = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false);
        IToolCallState toolCallState = _settings.CreateToolCallState();
        BlittableJsonReaderObject message = null;

        // need two contexts here because we run two parsing operations at once, first for each of the SSE events
        // and then for the internal buffer that there are providing.
        using var ___ = contextPool.AllocateOperationContext(out JsonOperationContext parsingContext);
        // Note that here we iterate over blittable, whose scope is only the _single_ iteration we run, you need to copy
        // any data that you need out of them.
        await foreach (var sseEvent in SseParser.Create(responseStream, (_, data) =>
                       {
                           unsafe
                           {
                               if (data.SequenceEqual("[DONE]"u8))
                                   return null;
                               fixed (byte* p = data)
                               {
                                   return parsingContext.ParseBuffer(p, data.Length, "msg", BlittableJsonDocumentBuilder.UsageMode.None);
                               }
                           }
                       }).EnumerateAsync(token))
        {
            using var _ = sseEvent.Data;

            if (sseEvent.Data is null) // "[DONE]"
            {
                toolCallState.AddAndReset();
                break;
            }

            trace?.CaptureSseEvent(streamingContext, sseEvent.Data);

            if (sseEvent.Data.TryGet(Constants.ResponseFields.Usage, out BlittableJsonReaderObject streamedUsage) && streamedUsage is not null)
            {
                usage.UpdateFrom(streamedUsage);
            }

            if (sseEvent.Data.TryGet(Constants.ResponseFields.Choices, out BlittableJsonReaderArray choices) is false ||
                choices.Length == 0)
            {
                continue;
            }

            var choice = (BlittableJsonReaderObject)choices[0];
            if (choice.TryGet(Constants.ResponseFields.Delta, out BlittableJsonReaderObject delta))
            {
                if (TryGetDeltaContent(delta, out LazyStringValue content))
                {
                    toolCallState.AddAndReset();

                    var final = parser.Process(content);
                    if (streamedPropertyBuffer.Length is not 0) // Length is the written data length (not the buffer real size)
                    {
                        // here we send all the data that wasn't sent so far to the client
                        await streamedPropertyCallback(streamedPropertyBuffer.AsMemory());
                        // reset the buffer length so we can overwrite the start of the buffer
                        // and only retain in memory the parts we'll need to send next time
                        streamedPropertyBuffer.Length = 0;
                    }

                    if (final is not null)
                    {
                        message = final;
                    }
                }

                if (delta.TryGet(Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray toolCalls))
                {
                    foreach (BlittableJsonReaderObject toolCallChunk in toolCalls)
                    {
                        toolCallState.Merge(toolCallChunk);
                    }
                }
            }
        }

        // Some OpenAI-like APIs return an empty array instead of omitting the field when no tool calls are made
        if (toolCallState.TryGetToolCallsForMessage(out var allToolCalls))
        {
            return new AiResponse(AiResponseType.Tool)
            {
                Message = streamingContext.ReadObject(new DynamicJsonValue
                {
                    [Constants.ResponseFields.Role] = Constants.RequestFields.RoleAssistantValue,
                    [Constants.ResponseFields.Content] = null,
                    [Constants.ResponseFields.ToolCalls] = allToolCalls
                }, "persisted/streamed/toolcalls"),
                ToolCalls = toolCallState.GetAllToolCalls(),
            };
        }

        return new AiResponse(AiResponseType.Result)
        {
            Message = streamingContext.ReadObject(new DynamicJsonValue
            {
                [Constants.ResponseFields.Role] = Constants.RequestFields.RoleAssistantValue,
                [Constants.ResponseFields.Content] = message,
            }, "persisted/streamed/message"),
            Result = message,
        };
    }

    private static bool TryGetDeltaContent(BlittableJsonReaderObject delta, out LazyStringValue content)
    {
        // Try content, then reasoning_content, then reasoning (for LM Studio and other reasoning model compatibility)
        if (delta.TryGet(Constants.ResponseFields.Content, out content) && content?.Length > 0)
            return true;

        if (delta.TryGet(Constants.ResponseFields.ReasoningContent, out content) && content?.Length > 0)
            return true;

        if (delta.TryGet(Constants.ResponseFields.Reasoning, out content) && content?.Length > 0)
            return true;

        content = null;
        return false;
    }

    public async Task<(string Result, string Message)> TestCompleteAsync(string systemPrompt, string userPrompt, string schema, CancellationToken token)
    {
        using var _ = _contextPool.AllocateOperationContext(out JsonOperationContext context);
        var prompt = context.ReadObject(new DynamicJsonValue
        {
            [Constants.RequestFields.Role] = Constants.RequestFields.RoleSystemValue,
            [Constants.RequestFields.Content] = systemPrompt
        }, "system/msg");

        var user = context.ReadObject(new DynamicJsonValue
        {
            [Constants.RequestFields.Role] = Constants.RequestFields.RoleUserValue,
            [Constants.RequestFields.Content] = userPrompt
        }, "system/msg");

        var request = CreateCompletionRequest(context, [prompt, user], attachments: null, tools: null, useTools: false, streaming: false, schema);
        var r = await CompleteAsync(context, request, new AiUsage(), trace: null, token);
        return (r.Result.ToString(), r.Message.ToString());
    }

    private const string AcceptsImageInputProbePngBase64 =
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=";

    public async Task<bool> TestAcceptsImageInputAsync(CancellationToken token)
    {
        try
        {
            using var _ = _contextPool.AllocateOperationContext(out JsonOperationContext context);

            var attachment = new AiAttachment("probe.png", Constants.AttachmentsRequestFields.MediaTypeImagePng, AiAttachmentSource.FromAttachment, AcceptsImageInputProbePngBase64);

            var userMessage = context.ReadObject(new DynamicJsonValue
            {
                [Constants.RequestFields.Role] = Constants.RequestFields.RoleUserValue,
                [Constants.RequestFields.Content] = "describe the image"
            }, "probe/user");

            var request = CreateCompletionRequest(context, messages: [userMessage], attachments: [attachment], tools: null, useTools: false, streaming: false, EmptySchema);
            await CompleteAsync(context, request, new AiUsage(), trace: null, token);
            return true;
        }
        catch (Exception) when (token.IsCancellationRequested == false)
        {
            return false;
        }
    }

    public async Task<AiResponse> CompleteAsync(JsonOperationContext context, HttpRequestMessage request, AiUsage usage, AiDebugTrace trace, CancellationToken token)
    {
        AddDefaultHeaders(request);
        using var response = await SendRequestAsync(request, token);
        var responseContent = await GetResponseContentAsync(context, response, token);

        trace?.CaptureResponse(responseContent);

        var responseParser = new AiResponseParser(this, response, responseContent);
        responseParser.EnsureSuccessfulResponse();
        responseParser.ParseMessage(usage);
        if (responseParser.TryParseToolCalls(out var tools))
        {
            return new AiResponse(AiResponseType.Tool) { ToolCalls = tools, Message = responseParser.Message };
        }

        var result = responseParser.GetContent(context);

        return new AiResponse(AiResponseType.Result) { Result = result, Message = responseParser.Message };
    }

    private struct AiResponseParser(ChatCompletionClient client, HttpResponseMessage response, BlittableJsonReaderObject responseContent)
    {
        public BlittableJsonReaderObject Message;
        private BlittableJsonReaderObject _choice0;

        public void EnsureSuccessfulResponse()
        {
            if (response.IsSuccessStatusCode)
                return;

            client.HandleUnsuccessfulResponse(response, responseContent);
            Debug.Assert(false, "we should never get here");
        }

        public void ParseMessage(AiUsage usage)
        {
            if (responseContent.TryGet(Constants.ResponseFields.Choices, out BlittableJsonReaderArray choices) == false || choices.Length == 0)
            {
                throw UnexpectedResponseException.Create(message: "No choices in response", response, responseContent);
            }

            _choice0 = (BlittableJsonReaderObject)choices[0];

            if (_choice0.TryGet(Constants.ResponseFields.Message, out Message) == false)
            {
                throw UnexpectedResponseException.Create(message: "No message property in choice", response, responseContent);
            }

            if (responseContent.TryGet(Constants.ResponseFields.Usage, out BlittableJsonReaderObject usageJson) == false)
                throw UnexpectedResponseException.Create(message: "No usage in response content", response, responseContent);
            usage.UpdateFrom(usageJson);
        }

        public bool TryParseToolCalls(out List<AiToolCall> toolCalls)
        {
            if (Message.TryGet(Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray calls) is false || calls.Length == 0)
            {
                toolCalls = null;
                return false;
            }

            toolCalls = [];
            foreach (BlittableJsonReaderObject call in calls)
            {
                if (call.TryGet(Constants.ResponseFields.Id, out string callId) is false ||
                    call.TryGet(Constants.ResponseFields.Function, out BlittableJsonReaderObject function) is false ||
                    function.TryGet(Constants.ResponseFields.Name, out string name) is false ||
                    function.TryGet(Constants.ResponseFields.Arguments, out string args) is false)
                    throw UnexpectedResponseException.Create(message: "Invalid function call: " + call, response, responseContent);
                toolCalls.Add(new AiToolCall(callId, name, args));
            }

            return true;
        }

        public BlittableJsonReaderObject GetContent(JsonOperationContext context)
        {
            // Try content, then reasoning_content, then reasoning (for LM Studio and other OpenAI-like APIs that still use the older mechanism)
            if (TryGetDeltaContent(Message, out var content) == false)
            {
                _choice0.TryGet(Constants.ResponseFields.FinishReason, out string finishReason);
                var refusal = client.GetRefusal(_choice0, Message);
                if (string.IsNullOrEmpty(refusal))
                    throw UnexpectedResponseException.Create(message: "No response content", response, responseContent);

                RefusedToAnswerException.Throw(refusal, responseContent.ToString(), finishReason, GetRequestId(response.Headers));
            }

            var result = context.Sync.ReadForMemory(content, "ai/output");
            Message.Modifications ??= new DynamicJsonValue(Message);
            Message.Modifications[Constants.ResponseFields.Content] = result;

            return result;
        }

    }

    protected virtual Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken token) => _client.SendAsync(request, token);

    protected virtual Task<HttpResponseMessage> SendStreamingRequestAsync(HttpRequestMessage request, CancellationToken token) => _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);

     public HttpRequestMessage CreateCompletionRequest(JsonOperationContext ctx,
        IEnumerable<BlittableJsonReaderObject> messages,
        List<AiAttachment> attachments,
        List<BlittableJsonReaderObject> tools,
        bool useTools,
        bool streaming,
        string schema,
        string promptCacheKey = null,
        AiDebugTrace trace = null)
    {
        if (_settings.Model is null)
            throw new ArgumentNullException(nameof(_settings.Model));

        trace?.CaptureAttachments(attachments);

        HttpContent content = new BlittableJsonContent(async stream =>
        {
            if (trace == null)
            {
                await WritePayloadAsync(stream).ConfigureAwait(false);
                return;
            }

            await using var target = new TeeStream(stream);
            try
            {
                await WritePayloadAsync(target).ConfigureAwait(false);
            }
            finally
            {
                trace.CaptureRequestBody(target.Result());
            }

            async Task WritePayloadAsync(Stream s)
            {
                await using var writer = new AsyncBlittableJsonTextWriter(ctx, s);
                if (_forTestingPurposes?.ModifyPayload != null)
                    _forTestingPurposes.ModifyPayload.Invoke(writer);
                else
                    WriteCompletionRequestPayload(writer, ctx, messages.Where(IsValidMessage),
                        attachments, tools, useTools, streaming, schema, promptCacheKey);
            }
        }, ConventionsToUse);

        content.Headers.Add(Constants.RequestFields.HeaderContentType, Constants.RequestFields.MediaTypeApplicationJson);

        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = content,
            RequestUri = new Uri(_settings.GetRelativeCompletionUri(), UriKind.Relative)
        };

        return request;

        static bool IsValidMessage(BlittableJsonReaderObject msg) 
            => msg.TryGet(Constants.ResponseFields.Role, out string role) == false || role != Constants.RequestFields.RoleInternalValue; // isn't an internal message
    }

    public void WriteCompletionRequestPayload(AsyncBlittableJsonTextWriter writer, JsonOperationContext ctx, IEnumerable<BlittableJsonReaderObject> messages, List<AiAttachment> attachments, List<BlittableJsonReaderObject> tools, bool useTools, bool streaming,
        string schema, string promptCacheKey = null)
    {
        writer.WriteStartObject();

        writer.WritePropertyName(Constants.RequestFields.Model);
        writer.WriteString(_settings.Model);
        writer.WriteComma();

        List<LazyStringValue> filterProperties = [ctx.GetLazyString(ConversationDocument.DateProperty), ctx.GetLazyString(ConversationDocument.UsageProperty)];

        writer.WriteArray(ctx, Constants.RequestFields.Messages, WithAttachments(ctx, messages, attachments), (w, context, message) =>
        {
            if (_forTestingPurposes?.SimulateFailureAsync != null)
                _forTestingPurposes.SimulateFailureAsync(message.ToString()).GetAwaiter().GetResult();

            w.WriteStartObject();
            w.WriteObjectWithFilter(message, filterProperties.Contains);
            w.WriteEndObject();
        });
        writer.WriteComma();

        // Optional
        if (tools?.Count > 0)
        {
            writer.WriteArray(Constants.RequestFields.Tools, tools);
            writer.WriteComma();

            if (useTools is false)
            {
                writer.WritePropertyName(Constants.RequestFields.ToolChoice);
                writer.WriteString("none");
                writer.WriteComma();
            }
        }

        writer.WritePropertyName(Constants.RequestFields.ResponseFormat);
        writer.WriteStartObject();
        writer.WritePropertyName(Constants.RequestFields.Type);
        writer.WriteString(Constants.RequestFields.JsonSchema);
        writer.WriteComma();
        writer.WritePropertyName(Constants.RequestFields.JsonSchema);
        writer.WriteObject(GetStructuredOutputSchemaAsBlittable());
        writer.WriteEndObject();

        if (streaming)
        {
            writer.WriteComma();
            writer.WritePropertyName(Constants.RequestFields.Stream);
            writer.WriteBool(true);
            writer.WriteComma();
            writer.WritePropertyName(Constants.RequestFields.StreamOptions);
            writer.WriteStartObject();
            writer.WritePropertyName(Constants.RequestFields.IncludeUsage);
            writer.WriteBool(true);
            writer.WriteEndObject();
        }

        if (promptCacheKey != null && _settings.EnablePromptCaching)
        {
            writer.WriteComma();
            writer.WritePropertyName(Constants.RequestFields.PromptCacheKey);
            writer.WriteString(promptCacheKey);
        }

        _settings.HandleCompletionRequestPayload(writer);

        writer.WriteEndObject();
        return;

        BlittableJsonReaderObject GetStructuredOutputSchemaAsBlittable()
        {
            using (var stream = RecyclableMemoryStreamFactory.GetRecyclableStream(Encoding.UTF8.GetBytes(schema)))
            {
                return ctx.Sync.ReadForMemory(stream, "json");
            }
        }
    }

    private IEnumerable<BlittableJsonReaderObject> WithAttachments(JsonOperationContext context, IEnumerable<BlittableJsonReaderObject> messages, List<AiAttachment> attachments)
    {
        foreach (var message in messages)
        {
            if (message.TryGet(Constants.RequestFields.Content, out object content))
            {
                // we need to stringify the content before sending to the model
                if (content is BlittableJsonReaderObject blittableJson)
                {
                    // clone once, not to change the original, since we are going to persist it
                    var msg = message.CloneOnTheSameContext();
                    var modifications = msg.Modifications ??= new DynamicJsonValue(msg);
                    modifications[Constants.RequestFields.Content] = blittableJson.ToString();
                    // clone twice, so the changes will take effect
                    yield return msg.CloneOnTheSameContext();
                    continue;
                }
            }

            yield return message;
        }

        if (attachments is not null && attachments.Count > 0)
        {
            var content = new DynamicJsonArray();
            var message = new DynamicJsonValue
            {
                [Constants.RequestFields.Role] = Constants.RequestFields.RoleUserValue,
                [Constants.RequestFields.Content] = content
            };

            foreach (var attachment in attachments)
            {
                if (attachment.Source == AiAttachmentSource.NotFound)
                {
                    content.Add(new DynamicJsonValue
                    {
                        [Constants.AttachmentsRequestFields.Type] = Constants.AttachmentsRequestFields.TypeText,
                        [Constants.AttachmentsRequestFields.TypeText] = $"File '{attachment.Name}' (of type '{attachment.Type}') could not be loaded: attachment not found"
                    });
                    continue;
                }

                content.Add(_settings.GetAiAttachmentJson(attachment));
            }
            yield return context.ReadObject(message, "write-ai-attachments");
        }
    }

    public async Task ProxyModelsAsync(HttpResponse response, CancellationToken token)
    {
        using var request = new HttpRequestMessage
        {
            Method = HttpMethod.Get,
            RequestUri = new Uri(_settings.GetRelativeModelsUri(), UriKind.Relative)
        };

        AddDefaultHeaders(request);
        using var r = await _client.SendAsync(request, token);

        HttpResponseHelper.CopyStatusCode(r, response);
        HttpResponseHelper.CopyHeaders(r, response);

        await HttpResponseHelper.CopyContentAsync(r, response);
    }

    private void AddDefaultHeaders(HttpRequestMessage request)
    {
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(Constants.RequestFields.MediaTypeApplicationJson));
        request.Headers.Authorization = string.IsNullOrEmpty(_settings.ApiKey) ? null : new AuthenticationHeaderValue(Constants.RequestFields.AuthorizationApiKeyProperty, _settings.ApiKey);

        _settings.AddHeaders(request);
    }

    public async Task<BlittableJsonReaderObject> GetResponseContentAsync(JsonOperationContext context, HttpResponseMessage response, CancellationToken token)
    {
        await using (var responseStream = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false))
        await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
        {
            await responseStream.CopyToAsync(ms, token);
            var contentLength = (int)ms.Position;
            ms.Position = 0;

            try
            {
                return await _settings.TryGetResponseContentAsync(context, ms).ConfigureAwait(false);
            }
            catch (Exception)
            {
                var rawBody = Encoding.UTF8.GetString(ms.GetBuffer(), 0, contentLength);
                throw UnexpectedResponseException.Create(message: "Received an unrecognized response from the server", response, rawBody);
            }
        }
    }

    [DoesNotReturn]
    private void HandleUnsuccessfulResponse(HttpResponseMessage response, BlittableJsonReaderObject content)
    {
        var headers = response.Headers;
        var reqId = GetRequestId(headers);

        var error = _settings.ParseError(content, response);
        var message = error.Message;

        switch (error.ErrorType)
        {
            case ErrorType.InsufficientQuota:
                throw new InsufficientQuotaException(message)
                {
                    RequestId = reqId
                };
            case ErrorType.Other429:
            case ErrorType.TooManyTokens:
            case ErrorType.TooManyRequests:
                var retryAfter = TimeSpan.Zero;
                if(headers.Contains(Constants.Headers.RetryAfterMs) == false &&
                  headers.Contains(Constants.Headers.RetryAfter) == false &&
                  error.RetryAfter == null)
                {
                    throw new TooManyTokensException(message)
                    {
                        RequestId = reqId
                    };
                }

                if (error.RetryAfter != null)
                    retryAfter = error.RetryAfter.Value;

                if (headers.TryGetValues(Constants.Headers.XRateLimitResetTokens, out var resetTokensValues))
                {
                    // TPM
                    var retryAfterAsString = resetTokensValues.FirstOrDefault();
                    if (TryParseResetTime(retryAfterAsString, out var retryAfterForTokens) == false)
                        throw new FormatException($"Unrecognized rate-limit format: '{retryAfterAsString}'");

                    retryAfter = retryAfterForTokens > retryAfter ? retryAfterForTokens : retryAfter;
                }

                if (headers.TryGetValues(Constants.Headers.XRateLimitResetRequests, out var resetRequestsValues))
                {
                    // RPM
                    var retryAfterAsString = resetRequestsValues.FirstOrDefault();
                    if (TryParseResetTime(retryAfterAsString, out var retryAfterForReqs) == false)
                        throw new FormatException($"Unrecognized rate-limit format: '{retryAfterAsString}'");

                    retryAfter = retryAfterForReqs > retryAfter ? retryAfterForReqs : retryAfter;
                }

                // TPM/RPM - should retry only for this exception
                throw new
                    RateLimitException(message)
                    {
                        RetryAfter = retryAfter,
                        RequestId = reqId
                    };
            case ErrorType.RefusedToAnswer:
                RefusedToAnswerException.Throw(message, content.ToString(), null, reqId);
                break;
            default:
                UnsuccessfulAiRequestException.Throw(content.ToString(), response.StatusCode, reqId);
                break;
        }
    }

    private string GetRefusal(BlittableJsonReaderObject choice0, BlittableJsonReaderObject message) => _settings.GetRefusal(choice0, message);

    internal static string GetRequestId(HttpResponseHeaders headers)
    {
        if (headers.TryGetValues(Constants.Headers.XRequestId, out IEnumerable<string> values))
        {
            return values.FirstOrDefault() ?? string.Empty;
        }

        // Azure API Management uses a different header name
        if (headers.TryGetValues("apim-request-id", out values))
        {
            return values.FirstOrDefault() ?? string.Empty;
        }

        return string.Empty;
    }

    private static readonly Regex GoDurationRegex = new(
        @"(?<value>\d+(?:\.\d+)?)(?<unit>ns|us|µs|ms|s|m|h)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant
    );

    internal static bool TryParseResetTime(string input, out TimeSpan time)
    {
        time = TimeSpan.Zero;

        if (string.IsNullOrEmpty(input))
            return false;

        // As int: 1684293600
        if (int.TryParse(input, out var seconds1))
        {
            time = TimeSpan.FromSeconds(seconds1);
            return true;
        }

        // As double: 33011.382867097855
        if (double.TryParse(input, provider: CultureInfo.InvariantCulture, out var seconds2))
        {
            time = TimeSpan.FromSeconds(seconds2);
            return true;
        }

        // As Duration (go style): 17ms, 1m8.754s, 5m, 1h
        var matches = GoDurationRegex.Matches(input);
        if (matches.Count == 0)
            return false;

        foreach (Match m in matches)
        {
            var v = double.Parse(m.Groups["value"].Value, CultureInfo.InvariantCulture);
            switch (m.Groups["unit"].Value)
            {
                case "h":
                    time += TimeSpan.FromHours(v);
                    break;
                case "m":
                    time += TimeSpan.FromMinutes(v);
                    break;
                case "s":
                    time += TimeSpan.FromSeconds(v);
                    break;
                case "ms":
                    time += TimeSpan.FromMilliseconds(v);
                    break;
                case "us":
                case "µs":
                    time += TimeSpan.FromTicks((long)(v * 10));
                    break; // 1 µs = 10 ticks
                case "ns":
                    time += TimeSpan.FromTicks((long)(v / 100));
                    break; // 1 ns = 1/100 tick
                default:
                    return false;
            }
        }

        return true;
    }

    public static string GetSchemaForRequest(string schema, string sampleObject)
    {
        if (string.IsNullOrWhiteSpace(schema) == false)
        {
            return schema;
        }

        if (string.IsNullOrWhiteSpace(sampleObject) == false)
        {
            return GetSchemaFromSampleObject(sampleObject);
        }

        throw new InvalidOperationException("Missing output schema and sample object in configuration (there must be at least one of them)");
    }

    public static string GetSchemaForTool(string schema, string sampleObject)
    {
        if (string.IsNullOrWhiteSpace(schema) == false)
        {
            return schema;
        }

        if (string.IsNullOrWhiteSpace(sampleObject) == false)
        {
            var doc = JsonDocument.Parse(sampleObject);
            var element = GenerateJsonSchemaObjectFromSampleObject(doc.RootElement);
            return JsonSerializer.Serialize(element, new JsonSerializerOptions { WriteIndented = true });
        }

        throw new InvalidOperationException("Missing output schema and sample object in configuration (there must be at least one of them)");
    }

    internal static string GetSchemaFromSampleObject(string sampleObject)
    {
        var doc = JsonDocument.Parse(sampleObject);

        var schema = new JsonObject
        {
            [Constants.JsonSchemaFields.Name] = GetAllowedUniqueName(sampleObject), // ensures a unique name
            [Constants.JsonSchemaFields.Strict] = true,
            [Constants.JsonSchemaFields.Schema] = GenerateJsonSchemaObjectFromSampleObject(doc.RootElement)
        };

        return JsonSerializer.Serialize(schema, new JsonSerializerOptions { WriteIndented = true });
    }

    private static JsonObject GenerateJsonSchemaObjectFromSampleObject(JsonElement element)
    {
        var jsonObj = new JsonObject();

        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                jsonObj[Constants.JsonSchemaFields.Type] = Constants.JsonSchemaFields.TypeObject;
                var props = new JsonObject();
                var required = new JsonArray();
                foreach (JsonProperty prop in element.EnumerateObject())
                {
                    props[prop.Name] = GenerateJsonSchemaObjectFromSampleObject(prop.Value);
                    required.Add(prop.Name);
                }

                jsonObj[Constants.JsonSchemaFields.Properties] = props;
                jsonObj[Constants.JsonSchemaFields.Required] = required;
                jsonObj[Constants.JsonSchemaFields.AdditionalProperties] = false;

                break;

            case JsonValueKind.Array:
                jsonObj[Constants.JsonSchemaFields.Type] = Constants.JsonSchemaFields.TypeArray;
                var content = element.EnumerateArray().FirstOrDefault();
                if (content.ValueKind is not JsonValueKind.Undefined)
                {
                    jsonObj[Constants.JsonSchemaFields.Items] = GenerateJsonSchemaObjectFromSampleObject(content);
                }
                else
                {
                    jsonObj[Constants.JsonSchemaFields.Items] = new JsonObject { [Constants.JsonSchemaFields.Type] = Constants.JsonSchemaFields.TypeNull, };
                }

                break;

            case JsonValueKind.String:
                jsonObj[Constants.JsonSchemaFields.Type] = Constants.JsonSchemaFields.TypeString;
                jsonObj[Constants.JsonSchemaFields.Description] = element.GetString();
                break;

            case JsonValueKind.Number:
                if (element.TryGetInt32(out _))
                {
                    jsonObj[Constants.JsonSchemaFields.Type] = Constants.JsonSchemaFields.TypeInteger;
                }
                else
                {
                    jsonObj[Constants.JsonSchemaFields.Type] = Constants.JsonSchemaFields.TypeNumber;
                }

                break;

            case JsonValueKind.True:
            case JsonValueKind.False:
                jsonObj[Constants.JsonSchemaFields.Type] = Constants.JsonSchemaFields.TypeBoolean;
                break;

            case JsonValueKind.Null:
                jsonObj[Constants.JsonSchemaFields.Type] = Constants.JsonSchemaFields.TypeNull;
                break;

            default:
                jsonObj[Constants.JsonSchemaFields.Type] = Constants.JsonSchemaFields.TypeNone;
                break;
        }

        return jsonObj;
    }

    public void Dispose()
    {
        DefaultRavenHttpClientFactory.Instance.TryRemoveHttpClient(_httpClientCacheKey);
    }

    internal static string GetAllowedUniqueName(string schemaOrSampleObject)
    {
        var hash = AttachmentsStorageHelper.CalculateHash(MemoryMarshal.AsBytes(schemaOrSampleObject.AsSpan()));
        return Base64UrlEncoder.Encode(Encoding.UTF8.GetBytes(hash));
    }

    public sealed class TestingStuff
    {
        internal TestingStuff()
        {
        }

        internal Action<AsyncBlittableJsonTextWriter> ModifyPayload;

        internal Func<string, Task> SimulateFailureAsync;
    }

    private TestingStuff _forTestingPurposes;

    public TestingStuff ForTestingPurposesOnly()
    {
        if (_forTestingPurposes != null)
            return _forTestingPurposes;

        return _forTestingPurposes = new TestingStuff();
    }

    public static class Constants
    {
        public static class ToolNames
        {
            // Internal RavenDB tool used by agents to fetch attachments from the conversation document.
            public const string RetrieveAttachment = "__RetrieveAttachment";
        }

        public static class ResponseFields
        {
            public const string Names = "names";
            public const string Choices = "choices";
            public const string Message = "message";
            public const string Content = "content";
            public const string ReasoningContent = "reasoning_content";
            public const string Reasoning = "reasoning";
            public const string FinishReason = "finish_reason";
            public const string ToolCalls = "tool_calls";
            public const string Refusal = "refusal";
            public const string Usage = "usage";
            public const string Error = "error";
            public const string ErrorCode = "code";
            public const string ErrorType = "type";
            public const string ErrorTypeInsufficientQuota = "insufficient_quota";
            public const string ErrorTypeTokens = "tokens";
            public const string ErrorTypeRequests = "requests";

            public const string Index = "index";
            public const string Id = "id";
            public const string Type = "type";
            public const string Function = "function";
            public const string Name = "name";
            public const string Arguments = "arguments";
            public const string Delta = "delta";
            public const string Role = "role";

            public const string ToolCallId = "tool_call_id";
            public const string SubConversationId = "subConversationId";
            public const string ToolName = "toolName";
        }

        public static class Headers
        {
            public const string RetryAfterMs = "retry-after-ms";
            public const string RetryAfter = "retry-after";
            public const string XRateLimitResetTokens = "x-ratelimit-reset-tokens";
            public const string XRateLimitResetRequests = "x-ratelimit-reset-requests";
            public const string XRequestId = "X-Request-ID";
        }

        public static class JsonSchemaFields
        {
            // Fields
            public const string Name = "name";
            public const string Strict = "strict";
            public const string Schema = "schema";
            public const string Type = "type";
            public const string AdditionalProperties = "additionalProperties";
            public const string Properties = "properties";
            public const string Required = "required";
            public const string Items = "items";
            public const string Description = "description";
            public const string Id = "id";
            public const string Function = "function";
            public const string Arguments = "arguments";
            public const string Parameters = "parameters";
            public const string Tool = "tool";

            // Values
            public const string TypeObject = "object";
            public const string TypeArray = "array";
            public const string TypeString = "string";
            public const string TypeInteger = "integer";
            public const string TypeNumber = "number";
            public const string TypeBoolean = "boolean";
            public const string TypeNull = "null";
            public const string TypeNone = "none";
        }

        public static class RequestFields
        {
            // JSON property names
            public const string Model = "model";
            public const string Messages = "messages";
            public const string Tools = "tools";
            public const string Role = "role";
            public const string Content = "content";
            public const string ResponseFormat = "response_format";
            public const string Type = "type";
            public const string JsonSchema = "json_schema";
            public const string Think = "think";
            public const string Temperature = "temperature";
            public const string ToolChoice = "tool_choice";
            public const string MaxCompletionToken = "max_completion_tokens";

            // JSON property values / enums
            public const string RoleSystemValue = "system";
            public const string RoleUserValue = "user";
            public const string RoleToolValue = "tool";
            public const string RoleAssistantValue = "assistant";
            public const string RoleInternalValue = "internal";

            // HTTP headers
            public const string HeaderContentType = "Content-Type";
            public const string MediaTypeApplicationJson = "application/json";

            public const string AuthorizationApiKeyProperty = "Bearer";

            public const string Stream = "stream";
            public const string StreamOptions = "stream_options";
            public const string IncludeUsage = "include_usage";
            public const string PromptCacheKey = "prompt_cache_key";
        }

        public static class AttachmentsRequestFields
        {
            public const string Type = "type";
            public const string File = "file";
            public const string FileName = "filename";
            public const string FileData = "file_data";
            public const string ImageUrl = "image_url";
            public const string Url = "url";

            public const string TypeText = "text";

            public const string MediaTypeTextPlain = "text/plain";
            public const string MediaTypeApplicationPdf = "application/pdf";
            public const string MediaTypeImageJpeg = "image/jpeg";
            public const string MediaTypeImagePng = "image/png";
            public const string MediaTypeImageGif = "image/gif";
            public const string MediaTypeImageWebp = "image/webp";
        }
    }
}
