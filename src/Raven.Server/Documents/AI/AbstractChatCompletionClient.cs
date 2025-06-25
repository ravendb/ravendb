using System;
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
using Microsoft.AspNetCore.Http;
using Microsoft.IdentityModel.Tokens;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Raven.Server.Documents.AI;

public abstract class AbstractChatCompletionClient<TContext> : IChatCompletionClient, IChatCompletionClientForTesting
    where TContext : JsonOperationContext
{
    private readonly string _model;
    private readonly string _organizationId;
    private readonly string _projectId;
    private readonly HttpClientCacheKey _httpClientCacheKey;
    private readonly HttpClient _client;
    private readonly string _structuredOutputSchema;
    private readonly DocumentConventions _conventions;
    private readonly JsonContextPoolBase<TContext> _contextPool;
    private readonly AuthenticationHeaderValue _auth;
    private readonly Uri _baseUri;

    protected AbstractChatCompletionClient(Uri baseUri, string model, string apiKey, string organizationId, string projectId, string structuredOutputSchema, JsonContextPoolBase<TContext> contextPool, DocumentConventions conventions)
    {
        _model = model;
        _organizationId = organizationId;
        _projectId = projectId;

        _auth = new AuthenticationHeaderValue(Constants.RequestFields.AuthorizationApiKeyProperty, apiKey);
        _baseUri = baseUri;
        _conventions = conventions;

        _httpClientCacheKey = new HttpClientCacheKey(certificate: null, _conventions.UseHttpDecompression,
            _conventions.HasExplicitlySetDecompressionUsage, _conventions.HttpPooledConnectionLifetime,
            _conventions.HttpPooledConnectionIdleTimeout, _conventions.GlobalHttpClientTimeout,
            httpClientType: GetType(), _conventions.ConfigureHttpMessageHandler);

        _client = DefaultRavenHttpClientFactory.Instance.GetHttpClient(_httpClientCacheKey, handler => new HttpClient(handler)
        {
            DefaultRequestHeaders =
            {
                Accept = { new MediaTypeWithQualityHeaderValue(Constants.RequestFields.MediaTypeApplicationJson) }
            }
        });

        _structuredOutputSchema = structuredOutputSchema ?? GetSchemaFor("{}");
        _contextPool = contextPool;
    }

    public async Task ProxyModelsAsync(HttpResponse response, CancellationToken token)
    {
        using var request = CreateRequest(HttpMethod.Get, Constants.RequestFields.ModelsUri);
        using var r = await _client.SendAsync(request, token);
        
        HttpResponseHelper.CopyStatusCode(r, response);
        HttpResponseHelper.CopyHeaders(r, response);

        await HttpResponseHelper.CopyContentAsync(r, response);
    }

    public async Task<(string Result, AiUsage Usage)> CompleteAsync(string prompt, string context, CancellationToken token)
    {
        if (_forTestingPurposes?.SimulateFailureAsync != null)
            await _forTestingPurposes.SimulateFailureAsync(context);

        using var _ = _contextPool.AllocateOperationContext(out JsonOperationContext ctx);
        using var request = CreateCompletionRequest(ctx, prompt, context);
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

        var aiUsage = new AiUsage();
        using (usage)
        {
            aiUsage.UpdateFrom(usage);
        }

        return (content, aiUsage);
    }

    private HttpRequestMessage CreateRequest(HttpMethod httpMethod, string relativeUri)
    {
        var request = new HttpRequestMessage
        {
            Method = httpMethod,
            RequestUri = new Uri(_baseUri, relativeUri),
            Headers =
            {
                Authorization = _auth
            }
        };

        if (string.IsNullOrEmpty(_organizationId) == false)
            request.Headers.TryAddWithoutValidation(Constants.RequestFields.OpenAiOrganization, _organizationId);
        
        if (string.IsNullOrEmpty(_projectId) == false)
            request.Headers.TryAddWithoutValidation(Constants.RequestFields.OpenAiProject, _projectId);

        return request;
    }

    private HttpRequestMessage CreateCompletionRequest(JsonOperationContext ctx, string prompt, string context)
    {
        var content = new BlittableJsonContent(async stream =>
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
            {
                if (_forTestingPurposes?.ModifyPayload != null)
                {
                    _forTestingPurposes?.ModifyPayload.Invoke(writer);
                    return;
                }

                writer.WriteStartObject();

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
        }, IChatCompletionClient.DefaultConventions);

        content.Headers.Add(Constants.RequestFields.HeaderContentType, Constants.RequestFields.MediaTypeApplicationJson);

        var request = CreateRequest(HttpMethod.Post, Constants.RequestFields.DefaultRelativeUri);
        request.Content = content;

        return request;

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

        JsonObject GenerateJsonSchemaObjectFromSampleObject(JsonElement element)
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
                        jsonObj[Constants.JsonSchemaFields.Items] = new JsonObject
                        {
                            [Constants.JsonSchemaFields.Type] = Constants.JsonSchemaFields.TypeNull,
                        };
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
            public const string OpenAiOrganization = "OpenAI-Organization";
            public const string OpenAiProject = "OpenAI-Project";
            public const string MediaTypeApplicationJson = "application/json";

            public const string DefaultRelativeUri = "/v1/chat/completions";
            public const string ModelsUri = "/v1/models";
            public const string AuthorizationApiKeyProperty = "Bearer";
        }
    }

}
