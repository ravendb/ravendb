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
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Tokens;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.Documents.AI.AiGen;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using JsonSerializer = System.Text.Json.JsonSerializer;


namespace Raven.Server.Documents.AI;

internal class ChatCompletionClient : IChatCompletionClient, IChatCompletionClientForTesting
{
    private readonly string _model;
    private readonly HttpClientCacheKey _httpClientCacheKey;
    private readonly HttpClient _client;
    private readonly string _structuredOutputSchema;
    private readonly IMemoryContextPool _contextPool;

    public static DocumentConventions ConventionsToUse = new DocumentConventions
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

    public static ChatCompletionClient CreateChatCompletionClient(IMemoryContextPool contextPool, AiConnectionString connection, string schema)
    {
        if (connection.TryGetParametersForGenAiTesting(out var uri, out var apiKey, out var model) == false)
        {
            var connectorType = connection.GetActiveProvider();
            throw new NotSupportedException($"The specified provider (\"{connectorType.ToString()}\") is not supported.");
        }

        var providerType = connection.GetActiveProviderInstance().GetType();

        return new ChatCompletionClient(contextPool, uri, apiKey, model, schema, providerType, ConventionsToUse);
    }

    public ChatCompletionClient(IMemoryContextPool contextPool, string baseUri, string apiKey, string model, string structuredOutputSchema, Type providerType, DocumentConventions conventions)
    {
        _model = model ?? throw new ArgumentNullException(nameof(model));

        _httpClientCacheKey = new HttpClientCacheKey(certificate: null, conventions.UseHttpDecompression,
            conventions.HasExplicitlySetDecompressionUsage, conventions.HttpPooledConnectionLifetime,
            conventions.HttpPooledConnectionIdleTimeout, conventions.GlobalHttpClientTimeout,
            httpClientType: providerType, conventions.ConfigureHttpMessageHandler);

        _client = DefaultRavenHttpClientFactory.Instance.GetHttpClient(_httpClientCacheKey, handler => new HttpClient(handler)
        {
            BaseAddress = new Uri(baseUri),
            DefaultRequestHeaders =
            {
                Authorization = string.IsNullOrEmpty(apiKey) ? null : new AuthenticationHeaderValue(Constants.RequestFields.AuthorizationApiKeyProperty, apiKey),
                Accept = { new MediaTypeWithQualityHeaderValue(Constants.RequestFields.MediaTypeApplicationJson) }
            }
        });

        _structuredOutputSchema = structuredOutputSchema ?? GetSchemaFor("{}");
        _contextPool = contextPool;
    }

    public async Task<AiResponse> CompleteAsync2(JsonOperationContext context, List<BlittableJsonReaderObject> messages, BlittableJsonReaderArray tools, AiUsage usage,
        CancellationToken token)
    {
        using var _ = _contextPool.AllocateOperationContext(out JsonOperationContext ctx);

        var bodyJson = new DynamicJsonValue
        {
            ["model"] = _model,
            ["messages"] = messages,
            ["tools"] = tools,
            ["response_format"] = new DynamicJsonValue
            {
                ["type"] = "json_schema", 
                ["json_schema"] = context.Sync.ReadForMemory(_structuredOutputSchema, "json/schema"),
            }
        };
        using var request = GetRequest2(ctx, bodyJson);
        using var response = await _client.SendAsync(request, token).ConfigureAwait(false);
        using var responseContent = await GetResponseContentAsync(ctx, response, token);

        if (response.IsSuccessStatusCode == false)
        {
            HandleUnsuccessfulResponse(response, responseContent);
            Debug.Assert(false, "we should never get here");
        }

        if (responseContent.TryGet("choices", out BlittableJsonReaderArray choices) == false || choices.Length == 0)
        {
            throw new UnexpectedResponseException("No choices in response: " + responseContent) { RequestId = GetRequestId(response.Headers) };
        }

        var choice0 = (BlittableJsonReaderObject)choices[0];
        
        if (choice0.TryGet("message", out BlittableJsonReaderObject msg) == false ||
            msg.TryGet("content", out string content) == false)
        {
            throw new UnexpectedResponseException("No message/content property in choice: " + responseContent) { RequestId = GetRequestId(response.Headers) };
        }
        
        messages.Add(context.ReadObject(msg,"copy-msg"));
        
        if (responseContent.TryGet("usage", out BlittableJsonReaderObject usageJson) == false)
            throw new UnexpectedResponseException("No choices property in response: " + responseContent) { RequestId = GetRequestId(response.Headers) };

        usage.UpdateFrom(usageJson);

        if (string.IsNullOrEmpty(content))
        {
            if (choice0.TryGet("finish_reason", out string finishReason) && 
                finishReason == "tool_calls" && 
                msg.TryGet("tool_calls", out BlittableJsonReaderArray calls))
            {
                var resp = new AiResponse(AiResponseType.Tool) { ToolCalls = [] };
                foreach (BlittableJsonReaderObject call in calls)
                {
                    if (call.TryGet("id", out string callId) is false ||
                        call.TryGet("function", out BlittableJsonReaderObject function) is false ||
                        function.TryGet("name", out string name) is false ||
                        function.TryGet("arguments", out string args) is false)
                        throw new InvalidOperationException("Invalid function call");//TODO: raise proper error with details here
                    resp.ToolCalls.Add(new AiToolCall(callId, name, args));
                }
                
                return resp;
            }
            
            choice0.TryGet("refusal", out string refusal); 
            //TODO: full output if we get here?
            throw new RefusedToAnswerException("The request was refused by the model")
            {
                Refusal = refusal, FinishReason = finishReason, RequestId = GetRequestId(response.Headers)
            };
        }

        var result = context.Sync.ReadForMemory(content, "ai/output");
        return new AiResponse(AiResponseType.Result) { Result = result };
    }

    public async Task<(string Result, string Usage)> CompleteAsync(string prompt, string context, CancellationToken token)
    {
        _forTestingPurposes?.SimulateFailure?.Invoke(context);

        using var _ = _contextPool.AllocateOperationContext(out JsonOperationContext ctx);
        using var request = GetRequest(ctx, prompt, context);
        using var response = await _client.SendAsync(request, token).ConfigureAwait(false);
        using var responseContent = await GetResponseContentAsync(ctx, response, token);

        if (response.IsSuccessStatusCode == false)
        {
            HandleUnsuccessfulResponse(response, responseContent);
            Debug.Assert(false, "we should never get here");
        }

        if (responseContent.TryGet(Constants.ResponseFields.Choices, out BlittableJsonReaderArray choices) == false || choices.Length == 0)
        {
            throw new UnexpectedResponseException("No choices in response: " + responseContent)
            {
                RequestId = GetRequestId(response.Headers)
            };
        }

        var choice0 = (BlittableJsonReaderObject)choices[0];
        if (choice0.TryGet(Constants.ResponseFields.Message, out BlittableJsonReaderObject msg) == false ||
             msg.TryGet(Constants.ResponseFields.Content, out string content) == false)
        {
            throw new UnexpectedResponseException("No message/content property in choice: " + responseContent)
            {
                RequestId = GetRequestId(response.Headers)
            };
        }

        if (string.IsNullOrEmpty(content))
        {
            choice0.TryGet(Constants.ResponseFields.FinishReason, out string finishReason);
            choice0.TryGet(Constants.ResponseFields.Refusal, out string refusal);

            throw new RefusedToAnswerException("The request was refused by the model")
            {
                Refusal = refusal,
                FinishReason = finishReason,
                RequestId = GetRequestId(response.Headers)
            };
        }

        if (responseContent.TryGet(Constants.ResponseFields.Usage, out BlittableJsonReaderObject usage) == false)
            throw new UnexpectedResponseException("No choices property in response: " + responseContent)
            {
                RequestId = GetRequestId(response.Headers)
            };

        return (content, usage.ToString());
    }

    private HttpRequestMessage GetRequest2(JsonOperationContext ctx, DynamicJsonValue body)
    {
        var content = new BlittableJsonContent(async stream =>
        {
            using var bodyJson = ctx.ReadObject(body, "ai-rag/request");
            await using var writer = new AsyncBlittableJsonTextWriter(ctx, stream);
            writer.WriteObject(bodyJson);
        }, ConventionsToUse);

        content.Headers.Add(Constants.RequestFields.HeaderContentType, Constants.RequestFields.MediaTypeApplicationJson);
        return new HttpRequestMessage
        {
            Method = HttpMethod.Post, 
            Content = content, 
            RequestUri = new Uri(Constants.RequestFields.DefaultRelativeUri, UriKind.Relative)
        };
    }

    private HttpRequestMessage GetRequest(JsonOperationContext ctx, string prompt, string context)
    {
        var content = new BlittableJsonContent(async stream =>
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
            {
                writer.WriteStartObject();

                if (_forTestingPurposes?.ModifyPayload != null)
                {
                    _forTestingPurposes?.ModifyPayload.Invoke(writer);
                    writer.WriteEndObject();
                }

                writer.WritePropertyName(Constants.RequestFields.Model);
                writer.WriteString(_model);
                writer.WriteComma();

                writer.WritePropertyName(Constants.RequestFields.Messages);
                writer.WriteStartArray();
                writer.WriteStartObject();
                writer.WritePropertyName(Constants.RequestFields.Role);
                writer.WriteString(Constants.RequestFields.RoleSystemValue);
                writer.WriteComma();
                writer.WritePropertyName(Constants.RequestFields.Content);
                writer.WriteString(prompt);
                writer.WriteEndObject();
                writer.WriteComma();
                writer.WriteStartObject();
                writer.WritePropertyName(Constants.RequestFields.Role);
                writer.WriteString(Constants.RequestFields.RoleUserValue);
                writer.WriteComma();
                writer.WritePropertyName(Constants.RequestFields.Content);
                writer.WriteString(context);
                writer.WriteEndObject();
                writer.WriteEndArray();
                writer.WriteComma();

                writer.WritePropertyName(Constants.RequestFields.ResponseFormat);
                writer.WriteStartObject();
                writer.WritePropertyName(Constants.RequestFields.Type);
                writer.WriteString(Constants.RequestFields.JsonSchema);
                writer.WriteComma();
                writer.WritePropertyName(Constants.RequestFields.JsonSchema);
                writer.WriteObject(GetStructuredOutputSchemaAsBlittable());
                writer.WriteEndObject();

                writer.WriteEndObject();
            }
        }, ConventionsToUse);

        content.Headers.Add(Constants.RequestFields.HeaderContentType, Constants.RequestFields.MediaTypeApplicationJson);

        return new HttpRequestMessage
        {
            Method = HttpMethod.Post, 
            Content = content, 
            RequestUri = new Uri(Constants.RequestFields.DefaultRelativeUri, UriKind.Relative)
        };

        BlittableJsonReaderObject GetStructuredOutputSchemaAsBlittable()
        {
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(_structuredOutputSchema)))
            {
                return ctx.Sync.ReadForMemory(stream, "json");
            }
        }
    }

    public virtual async Task<BlittableJsonReaderObject> GetResponseContentAsync(JsonOperationContext context, HttpResponseMessage response, CancellationToken token)
    {
        await using (var responseStream = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false))
        {
            var contentLength = response.Content.Headers.ContentLength;
            if (contentLength.HasValue && contentLength == 0)
                return null;

            // we intentionally don't dispose the reader here, we'll be using it
            // in the command, any associated memory will be released on context reset
            await using (var stream = new StreamWithTimeout(responseStream))
            {
                return await context.ReadForMemoryAsync(stream, "response/object").ConfigureAwait(false);
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

    public static string GenerateJsonObjectFromSampleObject(string s)
    {
        var doc = JsonDocument.Parse(s);
        var element = GenerateJsonSchemaObjectFromSampleObject(doc.RootElement);
        return JsonSerializer.Serialize(element, new JsonSerializerOptions { WriteIndented = true });
    }

    internal static string GetSchemaFor(string schemaOrSampleObject)
    {
        var doc = JsonDocument.Parse(schemaOrSampleObject);
        if (doc.RootElement.TryGetProperty(Constants.JsonSchemaFields.Type, out _) &&
            doc.RootElement.TryGetProperty(Constants.JsonSchemaFields.AdditionalProperties, out _) &&
            doc.RootElement.TryGetProperty(Constants.JsonSchemaFields.Properties, out _) &&
            doc.RootElement.TryGetProperty(Constants.JsonSchemaFields.Required, out _))
            return schemaOrSampleObject; // probably a schema, let's use that

        var schema = new JsonObject
        {
            [Constants.JsonSchemaFields.Name] = GetAllowedUniqueName(schemaOrSampleObject), // ensures a unique name
            [Constants.JsonSchemaFields.Strict] = true,
            [Constants.JsonSchemaFields.Schema] = GenerateJsonSchemaObjectFromSampleObject(doc.RootElement)
        };

        return JsonSerializer.Serialize(schema, new JsonSerializerOptions { WriteIndented = true });
    }

    public static JsonObject GenerateJsonSchemaObjectFromSampleObject(JsonElement element)
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

    private static class Constants
    {
        public static class ResponseFields
        {
            public const string Choices = "choices";
            public const string Message = "message";
            public const string Content = "content";
            public const string FinishReason = "finish_reason";
            public const string Refusal = "refusal";
            public const string Usage = "usage";
            public const string Error = "error";
            public const string ErrorType = "type";
            public const string ErrorTypeInsufficientQuota = "insufficient_quota";
            public const string ErrorTypeTokens = "tokens";
            public const string ErrorTypeRequests = "requests";
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
            public const string Role = "role";
            public const string Content = "content";
            public const string ResponseFormat = "response_format";
            public const string Type = "type";
            public const string JsonSchema = "json_schema";

            // JSON property values / enums
            public const string RoleSystemValue = "system";
            public const string RoleUserValue = "user";

            // HTTP headers
            public const string HeaderContentType = "Content-Type";
            public const string MediaTypeApplicationJson = "application/json";

            public const string DefaultRelativeUri = "/v1/chat/completions";
            public const string AuthorizationApiKeyProperty = "Bearer";
        }
    }

}
