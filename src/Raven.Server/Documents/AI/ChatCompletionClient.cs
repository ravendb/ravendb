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
using Raven.Client.Documents.AI;
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
using Sparrow.Exceptions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Raven.Server.Documents.AI;

public class ChatCompletionClient : IDisposable
{
    public static readonly string EmptySchema = GetSchemaFromSampleObject("{}");

    private readonly AbstractChatCompletionProvider _provider;
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
        if (AbstractChatCompletionProvider.TryGetParameters(connection, out var provider) == false)
        {
            var connectorType = connection.GetActiveProvider();
            throw new NotSupportedException($"The specified provider (\"{connectorType.ToString()}\") is not supported.");
        }

        return new ChatCompletionClient(contextPool, provider, ConventionsToUse);
    }

    internal ChatCompletionClient(IMemoryContextPool contextPool, AbstractChatCompletionProvider provider, DocumentConventions conventions = null)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));

        conventions ??= ConventionsToUse;

        var baseUri = provider.GetBaseEndpointUri();

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

    public async Task<AiResponse> StreamingCompleteAsync(JsonOperationContext streamingContext, IMemoryContextPool contextPool,
        string streamPropertyPath, AiChatRequest request,
        Func<Memory<byte>, Task> streamedPropertyCallback,
        AiUsage usage, AiDebugTrace trace, CancellationToken token)
    {
        request = WithoutInternalMessages(request);
        await SimulateRequestFailureIfNeededAsync(request);

        using var httpRequest = CreateCompletionRequest(streamingContext, request, streaming: true, trace);
        AddDefaultHeaders(httpRequest);
        using var streamedPropertyBuffer = new JsonOperationContextBuffer<byte>(streamingContext);

        bool structuredOutput = request.Schema != null;
        var alreadySeen = 0;

        using var parser = structuredOutput ? new SseStreamingJsonParser(streamingContext, streamPropertyPath) : null;
        if (parser != null)
        {
            // the `e` we get here is the _full_ string (including past chunks we already saw)
            parser.OnStringRead += (e) =>
            {
                alreadySeen += streamedPropertyBuffer.Append(alreadySeen, e);
            };
        }

        using var response = await SendStreamingRequestAsync(httpRequest, token);
        if (response.IsSuccessStatusCode == false)
        {
            var responseContent = await GetResponseContentAsync(streamingContext, response, token);
            HandleUnsuccessfulResponse(response, responseContent);
            Debug.Assert(false, "we should never get here");
        }

        await using var responseStream = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false);

        var state = _provider.CreateStreamState();
        state.Response = response;
        state.StructuredOutput = structuredOutput;
        state.Parser = parser;

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
                state.SawStop = true;
                break;
            }

            trace?.CaptureSseEvent(streamingContext, sseEvent.Data);

            var result = _provider.ProcessStreamEvent(parsingContext, sseEvent.Data, state, usage);

            if (result.TextDelta != null)
            {
                if (state.StructuredOutput)
                {
                    // nothing more is streamed once the parser has gone invalid; the finalizer reports it
                    var accepted = parser.TryProcess(result.TextDelta, out var final);
                    if (accepted && streamedPropertyBuffer.Length is not 0) // Length is the written data length (not the buffer real size)
                    {
                        // here we send all the data that wasn't sent so far to the client
                        await streamedPropertyCallback(streamedPropertyBuffer.AsMemory());
                        // reset the buffer length so we can overwrite the start of the buffer
                        // and only retain in memory the parts we'll need to send next time
                        streamedPropertyBuffer.Length = 0;
                    }

                    if (final is not null)
                        state.FinalResult = final;
                }
                else
                {
                    streamedPropertyBuffer.Append(result.TextDelta.AsSpan());
                    await streamedPropertyCallback(streamedPropertyBuffer.AsMemory());
                    streamedPropertyBuffer.Length = 0;
                }
            }

            if (result.Stop)
            {
                state.SawStop = true;
                break;
            }
        }

        var streamedResponse = _provider.BuildStreamedResponse(streamingContext, state, response);

        // A promoted reasoning fallback still has to reach the callback. In structured mode the parser already
        // wrote the property into the buffer while the finalizer replayed it, so only the flush is needed.
        if (state.PendingChunk != null)
        {
            if (state.StructuredOutput == false && state.PendingChunk.Length > 0)
                streamedPropertyBuffer.Append(streamingContext.GetLazyString(state.PendingChunk).AsSpan());

            if (streamedPropertyBuffer.Length is not 0)
            {
                await streamedPropertyCallback(streamedPropertyBuffer.AsMemory());
                streamedPropertyBuffer.Length = 0;
            }
        }

        return streamedResponse;
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

        var r = await CompleteAsync(context, new AiChatRequest { Messages = [prompt, user], Schema = schema }, new AiUsage(), trace: null, token);
        return (r.Result.ToString(), r.Message.ToString());
    }

    public async Task<bool> TestSupportsToolsAsync(CancellationToken token)
    {
        try
        {
            using var _ = _contextPool.AllocateOperationContext(out JsonOperationContext context);

            var userMessage = context.ReadObject(new DynamicJsonValue
            {
                [Constants.RequestFields.Role] = Constants.RequestFields.RoleUserValue,
                [Constants.RequestFields.Content] = "hi"
            }, "probe/user");

            var paramsSchema = GetSchemaForTool(schema: null, sampleObject: "{\"reason\":\"why the tool is being called\"}");
            var descriptors = new List<AiToolDescriptor>
            {
                new("connection_test_probe", "A probe so the request matches an agent call. Do not call it.", paramsSchema)
            };

            using var request = CreateCompletionRequest(context, new AiChatRequest
            {
                Messages = [userMessage],
                PreparedTools = PrepareTools(context, descriptors),
                UseTools = true,
                Schema = null
            }, streaming: false, trace: null);

            await EnsureRequestAcceptedAsync(context, request, token);
            return true;
        }
        catch (UnsuccessfulAiRequestException e) when (e.StatusCode == HttpStatusCode.BadRequest)
        {
            return false;
        }
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

            using var request = CreateCompletionRequest(context, new AiChatRequest
            {
                Messages = [userMessage],
                Attachments = [attachment],
                Schema = null
            }, streaming: false, trace: null);

            await EnsureRequestAcceptedAsync(context, request, token);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }
    
    private async Task EnsureRequestAcceptedAsync(JsonOperationContext context, HttpRequestMessage request, CancellationToken token)
    {
        AddDefaultHeaders(request);
        using var response = await SendRequestAsync(request, token);
        if (response.IsSuccessStatusCode)
            return;

        var responseContent = await GetResponseContentAsync(context, response, token);
        HandleUnsuccessfulResponse(response, responseContent);
    }

    public async Task<AiResponse> CompleteAsync(JsonOperationContext context, AiChatRequest request, AiUsage usage, AiDebugTrace trace, CancellationToken token)
    {
        request = WithoutInternalMessages(request);
        await SimulateRequestFailureIfNeededAsync(request);

        using var httpRequest = CreateCompletionRequest(context, request, streaming: false, trace);
        AddDefaultHeaders(httpRequest);
        using var response = await SendRequestAsync(httpRequest, token);
        var responseContent = await GetResponseContentAsync(context, response, token);

        trace?.CaptureResponse(responseContent);

        if (response.IsSuccessStatusCode == false)
        {
            HandleUnsuccessfulResponse(response, responseContent);
            Debug.Assert(false, "we should never get here");
        }

        return _provider.ParseResponse(context, response, responseContent, usage, structuredOutput: request.Schema != null);
    }

    protected virtual Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken token) => _client.SendAsync(request, token);

    protected virtual Task<HttpResponseMessage> SendStreamingRequestAsync(HttpRequestMessage request, CancellationToken token) => _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);

    private HttpRequestMessage CreateCompletionRequest(JsonOperationContext ctx, AiChatRequest request, bool streaming, AiDebugTrace trace)
    {
        if (_provider.Model is null)
            throw new ArgumentNullException(nameof(_provider.Model));

        trace?.CaptureAttachments(request.Attachments);

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
                    _provider.WritePayload(writer, ctx, request, streaming);
            }
        }, ConventionsToUse);

        content.Headers.Add(Constants.RequestFields.HeaderContentType, Constants.RequestFields.MediaTypeApplicationJson);

        var httpRequest = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = content,
            RequestUri = new Uri(_provider.GetRelativeCompletionUri(), UriKind.Relative)
        };

        return httpRequest;
    }

    // Internal messages are RavenDB bookkeeping and never reach a provider.
    private static AiChatRequest WithoutInternalMessages(AiChatRequest request) => request with { Messages = request.Messages.Where(IsValidMessage) };

    private static bool IsValidMessage(BlittableJsonReaderObject msg)
        => msg.TryGet(Constants.ResponseFields.Role, out string role) == false || role != Constants.RequestFields.RoleInternalValue; // isn't an internal message

    private async Task SimulateRequestFailureIfNeededAsync(AiChatRequest request)
    {
        var simulateFailure = _forTestingPurposes?.SimulateFailureAsync;
        if (simulateFailure == null)
            return;

        foreach (var message in request.Messages)
            await simulateFailure(message.ToString());
    }

    // Call once per conversation call, not per model iteration: the agent loop's context is not reset between
    // iterations, so every call adds another copy of the schema blittables to it. The result is only valid for ctx.
    public List<BlittableJsonReaderObject> PrepareTools(JsonOperationContext ctx, IReadOnlyList<AiToolDescriptor> descriptors)
    {
        if (descriptors is null || descriptors.Count == 0)
            return null;

        if (_forTestingPurposes != null)
            _forTestingPurposes.ToolPreparationCount++;

        var tools = new List<BlittableJsonReaderObject>(descriptors.Count);
        foreach (var descriptor in descriptors)
            tools.Add(ctx.ReadObject(_provider.BuildTool(ctx, descriptor.Name, descriptor.Description, descriptor.ParametersSchema), "tool"));

        return tools;
    }

    public async Task ProxyModelsAsync(HttpResponse response, CancellationToken token)
    {
        using var request = new HttpRequestMessage
        {
            Method = HttpMethod.Get,
            RequestUri = new Uri(_provider.GetRelativeModelsUri(), UriKind.Relative)
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
        _provider.AddAuthentication(request);
        _provider.AddHeaders(request);
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
                return await _provider.TryGetResponseContentAsync(context, ms).ConfigureAwait(false);
            }
            catch (Exception)
            {
                var rawBody = Encoding.UTF8.GetString(ms.GetBuffer(), 0, contentLength);
                throw UnexpectedResponseException.Create(message: "Received an unrecognized response from the server", response, rawBody, _provider.GetRequestId(response.Headers));
            }
        }
    }

    [DoesNotReturn]
    private void HandleUnsuccessfulResponse(HttpResponseMessage response, BlittableJsonReaderObject content)
    {
        var reqId = _provider.GetRequestId(response.Headers);

        var error = _provider.ParseError(content, response);
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
                var retryAfter = _provider.GetRetryAfter(response, error);
                if (retryAfter == null)
                    throw new TooManyTokensException(message)
                    {
                        RequestId = reqId
                    };

                throw new RateLimitException(message)
                {
                    RetryAfter = retryAfter.Value,
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

        // Replaces the request body for every provider; the provider's WritePayload is not called.
        internal Action<AsyncBlittableJsonTextWriter> ModifyPayload;

        internal Func<string, Task> SimulateFailureAsync;

        internal int ToolPreparationCount;

        internal AbstractChatCompletionProvider Provider;
    }

    private TestingStuff _forTestingPurposes;

    public TestingStuff ForTestingPurposesOnly()
    {
        if (_forTestingPurposes != null)
            return _forTestingPurposes;

        return _forTestingPurposes = new TestingStuff { Provider = _provider };
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
            public const string FinishReasonLength = "length";
            public const string FinishReasonContentFilter = "content_filter";
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
