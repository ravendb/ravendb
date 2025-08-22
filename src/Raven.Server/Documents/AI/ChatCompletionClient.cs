using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
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
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Raven.Server.Documents.AI;

internal class ChatCompletionClient : IChatCompletionClient, IChatCompletionClientForTesting
{
    public static readonly string EmptySchema = GetSchemaFromSampleObject("{}");

    private readonly string _model;
    private readonly string _organizationId;
    private readonly string _projectId;
    private readonly bool? _think;
    private readonly HttpClientCacheKey _httpClientCacheKey;
    private readonly HttpClient _client;
    private readonly IMemoryContextPool _contextPool;
    private readonly string _apiKey;

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
        if (connection.TryGetParametersForGenAiTesting(out var uri, out var apiKey, out var model, out var organizationId, out var projectId, out var think) == false)
        {
            var connectorType = connection.GetActiveProvider();
            throw new NotSupportedException($"The specified provider (\"{connectorType.ToString()}\") is not supported.");
        }

        return new ChatCompletionClient(contextPool, uri, apiKey, model, organizationId, projectId, think, ConventionsToUse);
    }

    internal ChatCompletionClient(IMemoryContextPool contextPool, string baseUri, string apiKey, string model, string organizationId, string projectId, bool? think = null, DocumentConventions conventions = null)
    {
        _model = model;
        _organizationId = organizationId;
        _projectId = projectId;
        _think = think;

        conventions ??= ConventionsToUse;

        _httpClientCacheKey = HttpClientCacheKey.Create(conventions.UseHttpDecompression,
            conventions.HasExplicitlySetDecompressionUsage, conventions.HttpPooledConnectionLifetime,
            conventions.HttpPooledConnectionIdleTimeout, conventions.GlobalHttpClientTimeout,
            baseUri, conventions.ConfigureHttpMessageHandler);

        _client = DefaultRavenHttpClientFactory.Instance.GetHttpClient(_httpClientCacheKey, handler => new HttpClient(handler)
        {
            BaseAddress = new Uri(baseUri),
        });

        _contextPool = contextPool;
        _apiKey = apiKey;
    }


    private struct ToolCallState
    {
        private StringBuilder _id;
        private StringBuilder _type;
        private StringBuilder _name;
        private StringBuilder _arguments;

        private int _toolCallIndex;

        public List<AiToolCall> AllToolCalls;

        public ToolCallState()
        {
            _toolCallIndex = -1;
        }

        public void Merge(BlittableJsonReaderObject toolCallChunk)
        {
            if (!toolCallChunk.TryGet(Constants.ResponseFields.Index, out int index))
                return;

            if (index != _toolCallIndex)
            {
                AddAndReset();
                _toolCallIndex = index;
            }

            if (toolCallChunk.TryGet(Constants.ResponseFields.Id, out string id))
            {
                _id.Append(id);
            }

            if (toolCallChunk.TryGet(Constants.ResponseFields.Type, out string type))
            {
                _type.Append(type);
            }

            if (toolCallChunk.TryGet(Constants.ResponseFields.Function, out BlittableJsonReaderObject functionChunk))
            {
                if (functionChunk.TryGet(Constants.ResponseFields.Name, out string nameChunk))
                {
                    _name.Append(nameChunk);
                }

                if (functionChunk.TryGet(Constants.ResponseFields.Arguments, out string argsChunk))
                {
                    _arguments.Append(argsChunk);
                }
            }
        }

        public void AddAndReset()
        {
            if (_toolCallIndex == -1)
            {
                _id ??= new();
                _type ??= new();
                _name ??= new();
                _arguments ??= new();

                return;
            }

            AllToolCalls ??= [];
            AllToolCalls.Add(new AiToolCall(_id.ToString(), _name.ToString(), _arguments.ToString()));


            _toolCallIndex = -1;
            _id.Clear();
            _type.Clear();
            _name.Clear();
            _arguments.Clear();
        }
    }


    public async Task<AiResponse> StreamingCompleteAsync(JsonOperationContext streamingContext, IMemoryContextPool contextPool,
        string propertyToStream, HttpRequestMessage request,
        Func<Memory<byte>, Task> streamedPropertyCallback,
        AiUsage usage, CancellationToken token)
    {
        AddDefaultHeaders(request);
        // we use a small buffer size since we expect those to be "token" level updates, not very big ones
        const int initialBufferSize = 64;

        using var __ = streamingContext.GetRawMemoryBuffer(initialBufferSize, out var streamedPropertyBuffer);
        var parser = new SseStreamingJsonParser(streamingContext, propertyToStream);
        var alreadySeen = 0;
        var sizeToStream = 0;
        parser.OnStringRead += (e) =>
        {
            int size = e.SizeInBytes - alreadySeen;
            if (size > streamedPropertyBuffer.SizeInBytes)
            {
                streamingContext.GrowAllocation(streamedPropertyBuffer, size - streamedPropertyBuffer.SizeInBytes);
            }

            unsafe
            {
                var read = e.CopyTo(alreadySeen, streamedPropertyBuffer.Address);
                alreadySeen += read;
                sizeToStream += read;
            }
        };

        using var response = await SendStreamingRequestAsync(request, token);
        if (response.IsSuccessStatusCode == false)
        {
            var responseContent = await GetResponseContentAsync(streamingContext, response, token);
            HandleUnsuccessfulResponse(response, responseContent);
            Debug.Assert(false, "we should never get here");
        }

        await using var responseStream = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false);
        ToolCallState toolCallState = new();
        BlittableJsonReaderObject message = null;

        // need to contexts here because we run two parsing operations at once, first for each of the SSE events
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
                if (delta.TryGet(Constants.ResponseFields.Content, out LazyStringValue content) && content?.Length > 0)
                {
                    toolCallState.AddAndReset();

                    var final = parser.Process(content);
                    if (sizeToStream is not 0)
                    {
                        await streamedPropertyCallback(streamedPropertyBuffer.AsMemory()[..sizeToStream]);
                        sizeToStream = 0;
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

        if (toolCallState.AllToolCalls?.Count >= 0)
        {
            DynamicJsonArray toolCalls = new();
            foreach (var call in toolCallState.AllToolCalls)
            {
                toolCalls.Add(new DynamicJsonValue
                {
                    [Constants.ResponseFields.Id] = call.Id,
                    [Constants.ResponseFields.Type] = Constants.ResponseFields.Function,
                    [Constants.ResponseFields.Function] = new DynamicJsonValue
                    {
                        [Constants.ResponseFields.Name] = call.Name,
                        [Constants.ResponseFields.Arguments] = call.Arguments
                    }
                });
            }

            return new AiResponse(AiResponseType.Tool)
            {
                Message = streamingContext.ReadObject(new DynamicJsonValue
                {
                    [Constants.ResponseFields.Role] = Constants.RequestFields.RoleAssistantValue,
                    [Constants.ResponseFields.Content] = null,
                    [Constants.ResponseFields.ToolCalls] = toolCalls
                }, "persisted/streamed/toolcalls"),
                ToolCalls = toolCallState.AllToolCalls,
            };
        }

        return new AiResponse(AiResponseType.Result)
        {
            Message = streamingContext.ReadObject(new DynamicJsonValue
            {
                [Constants.ResponseFields.Role] = Constants.RequestFields.RoleAssistantValue,
                [Constants.ResponseFields.Content] = message!.ToString(),
            }, "persisted/streamed/message"),
            Result = message,
        };
    }

    public async Task<AiResponse> CompleteAsync(JsonOperationContext context, HttpRequestMessage request, AiUsage usage, CancellationToken token)
    {
        AddDefaultHeaders(request);
        using var response = await SendRequestAsync(request, token);
        var responseContent = await GetResponseContentAsync(context, response, token);

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
        private string _content;
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
                throw new UnexpectedResponseException("No choices in response: " + responseContent) { RequestId = GetRequestId(response.Headers) };
            }

            _choice0 = (BlittableJsonReaderObject)choices[0];

            if (_choice0.TryGet(Constants.ResponseFields.Message, out Message) == false ||
                Message.TryGet(Constants.ResponseFields.Content, out _content) == false)
            {
                throw new UnexpectedResponseException("No message/content property in choice: " + responseContent) { RequestId = GetRequestId(response.Headers) };
            }

            if (responseContent.TryGet(Constants.ResponseFields.Usage, out BlittableJsonReaderObject usageJson) == false)
                throw new UnexpectedResponseException("No choices property in response: " + responseContent) { RequestId = GetRequestId(response.Headers) };

            usage.UpdateFrom(usageJson);
        }

        public bool TryParseToolCalls(out List<AiToolCall> toolCalls)
        {
            if (Message.TryGet(Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray calls) is false)
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
                    throw new UnexpectedResponseException("Invalid function call: " + call)
                    {
                        RequestId = GetRequestId(response.Headers)
                    };
                toolCalls.Add(new AiToolCall(callId, name, args));
            }

            return true;
        }

        public BlittableJsonReaderObject GetContent(JsonOperationContext context)
        {
            if (string.IsNullOrEmpty(_content))
            {
                _choice0.TryGet(Constants.ResponseFields.FinishReason, out string finishReason);
                _choice0.TryGet(Constants.ResponseFields.Refusal, out string refusal);
                //TODO: full output if we get here?
                throw new RefusedToAnswerException("The request was refused by the model")
                {
                    Refusal = refusal, FinishReason = finishReason, RequestId = GetRequestId(response.Headers)
                };
            }

            return context.Sync.ReadForMemory(_content, "ai/output");
        }
    }

    protected virtual Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken token) => _client.SendAsync(request, token);

    protected virtual Task<HttpResponseMessage> SendStreamingRequestAsync(HttpRequestMessage request, CancellationToken token) => _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);

    public async Task<(string Result, AiUsage Usage)> CompleteAsync(string systemPrompt, string userPrompt, string schema, CancellationToken token)
    {
        if (_forTestingPurposes?.SimulateFailureAsync != null)
            await _forTestingPurposes.SimulateFailureAsync(userPrompt);

        using var _ = _contextPool.AllocateOperationContext(out JsonOperationContext ctx);

        var msg1 = new DynamicJsonValue
        {
            [Constants.RequestFields.Role] = Constants.RequestFields.RoleSystemValue,
            [Constants.RequestFields.Content] = systemPrompt
        };
        var msg2 = new DynamicJsonValue
        {
            [Constants.RequestFields.Role] = Constants.RequestFields.RoleUserValue,
            [Constants.RequestFields.Content] = userPrompt
        };

        var messages = new List<BlittableJsonReaderObject>() { ctx.ReadObject(msg1, "system/msg"), ctx.ReadObject(msg2, "user/msg") };
        using var request = CreateCompletionRequest(ctx, messages, schema);
        var usage = new AiUsage();
        var results = await CompleteAsync(ctx, request, usage, token);

        return (results.Result.ToString(), usage);
    }

    public HttpRequestMessage CreateCompletionRequest(JsonOperationContext ctx, List<BlittableJsonReaderObject> messages, string schema) => CreateCompletionRequest(ctx, messages, tools: null, useTools: false, streaming: false, schema);

    public HttpRequestMessage CreateCompletionRequest(JsonOperationContext ctx,
        List<BlittableJsonReaderObject> messages,
        List<BlittableJsonReaderObject> tools,
        bool useTools,
        bool streaming,
        string schema)
    {
        if (_model is null)
            throw new ArgumentNullException(nameof(_model));

        var content = new BlittableJsonContent(async stream =>
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
            {
                if (_forTestingPurposes?.ModifyPayload != null)
                {
                    _forTestingPurposes?.ModifyPayload.Invoke(writer);
                    return;
                }

                WriteCompletionRequestPayload(writer, ctx, messages, tools, useTools, streaming, schema);
            }
        }, ConventionsToUse);

        content.Headers.Add(Constants.RequestFields.HeaderContentType, Constants.RequestFields.MediaTypeApplicationJson);

        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = content,
            RequestUri = new Uri(Constants.RequestFields.DefaultRelativeUri, UriKind.Relative)
        };

        return request;
    }

    public void WriteCompletionRequestPayload(AsyncBlittableJsonTextWriter writer, JsonOperationContext ctx, IEnumerable<BlittableJsonReaderObject> messages, List<BlittableJsonReaderObject> tools, bool useTools, bool streaming, string schema)
    {
        writer.WriteStartObject();

        writer.WritePropertyName(Constants.RequestFields.Model);
        writer.WriteString(_model);
        writer.WriteComma();

        List<LazyStringValue> filterProperties = [ctx.GetLazyString(ConversationDocument.DateProperty), ctx.GetLazyString(ConversationDocument.UsageProperty)];

        writer.WriteArray(ctx, Constants.RequestFields.Messages, messages, (w, context, message) =>
        {
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

        // Add Ollama-specific "think" parameter if specified
        if (_think.HasValue)
        {
            writer.WriteComma();
            writer.WritePropertyName(Constants.RequestFields.Think);
            writer.WriteBool(_think.Value);
        }

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

    public async Task ProxyModelsAsync(HttpResponse response, CancellationToken token)
    {
        using var request = new HttpRequestMessage
        {
            Method = HttpMethod.Get,
            RequestUri = new Uri(Constants.RequestFields.ModelsUri, UriKind.Relative)
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
        request.Headers.Authorization = string.IsNullOrEmpty(_apiKey) ? null : new AuthenticationHeaderValue(Constants.RequestFields.AuthorizationApiKeyProperty, _apiKey);

        if (string.IsNullOrEmpty(_organizationId) == false)
            request.Headers.TryAddWithoutValidation(Constants.RequestFields.OpenAiOrganization, _organizationId);

        if (string.IsNullOrEmpty(_projectId) == false)
            request.Headers.TryAddWithoutValidation(Constants.RequestFields.OpenAiProject, _projectId);
    }

    public async Task<BlittableJsonReaderObject> GetResponseContentAsync(JsonOperationContext context, HttpResponseMessage response, CancellationToken token)
    {
        await using (var responseStream = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false))
        await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
        {
            await responseStream.CopyToAsync(ms, token);
            ms.Position = 0;
            try
            {
                return await context.ReadForMemoryAsync(ms, "response/object").ConfigureAwait(false);
            }
            catch (Exception e)
            {
                ms.Position = 0;
                string content = Encoding.UTF8.GetString(ms.GetMemory().Span);
                throw new UnexpectedResponseException($"Got unrecognized response from the server: {content}. {response.StatusCode}", e)
                {
                    RequestId = GetRequestId(response.Headers)
                };
            }
        }
    }

    [DoesNotReturn]
    private void HandleUnsuccessfulResponse(HttpResponseMessage response, BlittableJsonReaderObject responseContent)
    {
        var headers = response.Headers;
        var reqId = GetRequestId(headers);

        if (responseContent.TryGet(Constants.ResponseFields.Error, out BlittableJsonReaderObject errBjo) is false || errBjo.TryGet(Constants.ResponseFields.Message, out string message) is false)
            throw new UnexpectedResponseException("Unexpected response: " + responseContent)
            {
                RequestId = reqId
            };

        switch (response.StatusCode)
        {
            case HttpStatusCode.TooManyRequests:

                if (errBjo.TryGet(Constants.ResponseFields.ErrorType, out string type) == false)
                    throw new UnexpectedResponseException($"No type specified (status {HttpStatusCode.TooManyRequests}): " + responseContent)
                    {
                        RequestId = reqId
                    };

                switch (type)
                {
                    case Constants.ResponseFields.ErrorTypeInsufficientQuota:
                        throw new InsufficientQuotaException(message)
                        {
                            RequestId = reqId
                        };

                    case Constants.ResponseFields.ErrorTypeTokens:
                    case Constants.ResponseFields.ErrorTypeRequests:

                        var retryAfter = TimeSpan.Zero;
                        if (headers.Contains(Constants.Headers.RetryAfter) == false)
                        {
                            throw new TooManyTokensException(message)
                            {
                                RequestId = reqId
                            };
                        }

                        if (headers.TryGetValues(Constants.Headers.TokensResetTime, out var resetTokensValues))
                        {
                            // TPM
                            var retryAfterAsString = resetTokensValues.FirstOrDefault();
                            if (TryParseResetTime(retryAfterAsString, out retryAfter) == false)
                                throw new FormatException($"Unrecognized rate-limit format: '{retryAfterAsString}'");
                        }

                        if (headers.TryGetValues(Constants.Headers.RequestsResetTime, out var resetRequestsValues))
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
                    default:
                        throw new TooManyRequestsException(message)
                        {
                            RequestId = reqId
                        };
                }
            default:
                throw new UnsuccessfulRequestException(message, response.StatusCode)
                {
                    RequestId = reqId
                };
        }
    }

    private static string GetRequestId(HttpResponseHeaders headers)
    {
        if (headers.TryGetValues(Constants.Headers.RequestId, out var values) == false || values.IsNullOrEmpty())
        {
            return string.Empty;
        }

        return values.FirstOrDefault();
    }

    private static bool TryParseResetTime(string input, out TimeSpan time)
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
        var matches = IChatCompletionClient.GoDurationRegex.Matches(input);
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

    private IChatCompletionClientForTesting.TestingStuff _forTestingPurposes;

    public IChatCompletionClientForTesting.TestingStuff ForTestingPurposesOnly()
    {
        if (_forTestingPurposes != null)
            return _forTestingPurposes;

        return _forTestingPurposes = new IChatCompletionClientForTesting.TestingStuff();
    }

    public static class Constants
    {
        public static class ResponseFields
        {
            public const string Choices = "choices";
            public const string Message = "message";
            public const string Content = "content";
            public const string FinishReason = "finish_reason";
            public const string ToolCalls = "tool_calls";
            public const string Refusal = "refusal";
            public const string Usage = "usage";
            public const string Error = "error";
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
        }

        public static class Headers
        {
            public const string RetryAfter = "retry-after-ms";
            public const string TokensResetTime = "x-ratelimit-reset-tokens";
            public const string RequestsResetTime = "x-ratelimit-reset-requests";
            public const string RequestId = "X-Request-ID";
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
            public const string ToolChoice = "tool_choice";
            public const string MaxCompletionToken = "max_completion_tokens";

            // JSON property values / enums
            public const string RoleSystemValue = "system";
            public const string RoleUserValue = "user";
            public const string RoleAssistantValue = "assistant";

            // HTTP headers
            public const string HeaderContentType = "Content-Type";
            public const string OpenAiOrganization = "OpenAI-Organization";
            public const string OpenAiProject = "OpenAI-Project";
            public const string MediaTypeApplicationJson = "application/json";

            public const string DefaultRelativeUri = "/v1/chat/completions";
            public const string ModelsUri = "/v1/models";
            public const string AuthorizationApiKeyProperty = "Bearer";
            
            public const string Stream = "stream";
            public const string StreamOptions = "stream_options";
            public const string IncludeUsage = "include_usage";
        }
    }
}
