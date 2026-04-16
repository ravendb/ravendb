using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.AI.Settings;

internal class GoogleChatCompletionClientSettings : AbstractOpenAiChatCompletionClientSettings
{
    private readonly string _aiVersion;
    public override bool SupportStrictTools => false;  // Google AI does not support strict tools, so we set this to false by default.
    public override bool EnablePromptCaching => _settings.EnablePromptCache ?? false; // Google AI returns errors if we send this

    public GoogleChatCompletionClientSettings(GoogleSettings settings) : base(settings)
    {
        var aiVersion = settings.AiVersion ?? GoogleAIVersion.V1_Beta; // if AiVersion is not set, default to V1_Beta
        _aiVersion = GetAiVersion(aiVersion);
    }

    public override string GetRelativeCompletionUri() => _aiVersion + "/openai/" + base.GetRelativeCompletionUri();

    public override string GetRelativeModelsUri() => _aiVersion + "/openai/" + base.GetRelativeModelsUri();

    private string GetAiVersion(GoogleAIVersion aiVersion)
    {
        switch (aiVersion)
        {
            case GoogleAIVersion.V1:
                return "v1";
            case GoogleAIVersion.V1_Beta:
                return "v1beta";
            default:
                throw new ArgumentOutOfRangeException(nameof(aiVersion), aiVersion, null);
        }
    }

    internal override IToolCallState CreateToolCallState()
    {
        return new GoogleToolCallState();
    }

    public override AiError ParseError(BlittableJsonReaderObject content, HttpResponseMessage response)
    {
        var holder = GoogleErrorHolder.Deserializer(content);
        var error = holder?.error;

        if (error == null)
        {
            return new AiError
            {
                ErrorType = ErrorType.Unknown
            };
        }

        var statusCode = response.StatusCode;
        var status = error.status ?? string.Empty;

        ErrorType errorType = ErrorType.Unknown;
        TimeSpan? retryAfter = null;

        switch (statusCode)
        {
            case HttpStatusCode.BadRequest:
                if (status == "INVALID_ARGUMENT")
                {
                    // Gemini does not provide a real subtype for INVALID_ARGUMENT.
                    // We treat it as TooManyTokens only if the error is token-related.
                    var msg = error.message ?? string.Empty;

                    if (msg.Contains("token", StringComparison.OrdinalIgnoreCase) ||
                        msg.Contains("maxOutputTokens", StringComparison.OrdinalIgnoreCase))
                        errorType = ErrorType.TooManyTokens;
                }
                break;
            case HttpStatusCode.TooManyRequests:
                if (status == "RESOURCE_EXHAUSTED")
                {
                    // parse '@type'
                    if (error.details?.Count > 0 && content.TryGet("error", out BlittableJsonReaderObject errorBjro)
                                                 && errorBjro.TryGet("details", out BlittableJsonReaderArray detailsBjra))
                    {
                        for (int i = 0; i < error.details.Count; i++)
                        {
                            if (detailsBjra[i] is BlittableJsonReaderObject d && d.TryGet("@type", out string t))
                                error.details[i].Type = t;
                        }
                    }

                    errorType = ParseRateLimit(error, out retryAfter);
                }
                else
                    errorType = ErrorType.Other429;
                break;
            default:
                errorType = ErrorType.Unknown;
                break;
        }

        return new AiError
        {
            ErrorType = errorType,
            Message = error.message, // Informational only (not used for classification)
            RetryAfter = retryAfter,
        };
    }

    private static ErrorType ParseRateLimit(GoogleError error, out TimeSpan? retryAfter)
    {
        retryAfter = null;
        if (error.details == null || error.details.Count == 0)
            return ErrorType.Other429;

        var rateLimitType = ErrorType.Other429;

        foreach (var d in error.details)
        {
            switch (d.Type)
            {
                case "type.googleapis.com/google.rpc.RetryInfo":
                    if (ChatCompletionClient.TryParseResetTime(d.retryDelay, out var retry))
                        retryAfter = retry;
                    break;


                case "type.googleapis.com/google.rpc.QuotaFailure":
                    if (rateLimitType != ErrorType.Other429)
                        break;

                    if (d.violations == null)
                        break;

                    foreach (var v in d.violations)
                    {
                        var quotaId = v.quotaId ?? string.Empty;

                        if (quotaId.Contains("InputTokens", StringComparison.OrdinalIgnoreCase) ||
                            quotaId.Contains("PerMinute", StringComparison.OrdinalIgnoreCase))
                        {
                            rateLimitType = ErrorType.TooManyTokens;
                            break;
                        }

                        if (quotaId.Contains("PerDay", StringComparison.OrdinalIgnoreCase) ||
                            quotaId.Contains("PaidTier", StringComparison.OrdinalIgnoreCase))
                        {
                            rateLimitType = ErrorType.InsufficientQuota;
                            break;
                        }
                    }

                    break;
            }
        }

        return rateLimitType;
    }

    private class GoogleError
    {
        public string status { get; set; }
        public string message { get; set; }
        public List<GoogleErrorDetail> details { get; set; }
    }

    private class GoogleErrorHolder
    {
        public static readonly Func<BlittableJsonReaderObject, GoogleErrorHolder> Deserializer = JsonDeserializationBase.GenerateJsonDeserializationRoutine<GoogleErrorHolder>();
        public GoogleError error { get; set; }
    }

    private class GoogleErrorDetail
    {
        public string Type { get; set; }
        public List<GoogleQuotaViolation> violations { get; set; }
        public string retryDelay { get; set; }
    }

    private class GoogleQuotaViolation
    {
        public string quotaMetric { get; set; }
        public string quotaId { get; set; }
        public string quotaValue { get; set; }
    }

    public override string GetRefusal(BlittableJsonReaderObject choice0, BlittableJsonReaderObject message)
    {
        // Google’s OpenAI‑compatible API does not include a "refusal" field in safety‑blocked responses.
        // When the model refuses to answer (e.g., due to safety rules), the response looks like:
        //
        // {
        //     "choices": [
        //         {
        //             "index": 0,
        //             "message": { "role": "assistant" }
        //         }
        //     ],
        //     "usage": { "completion_tokens": 0, ... }
        // }
        //
        // The "message" object contains no "content" and no "refusal" metadata.
        // Because there is nothing to extract from the JSON, we return a fixed refusal message.
        return "The model refused to answer";
    }

    public override async ValueTask<BlittableJsonReaderObject> TryGetResponseContentAsync(JsonOperationContext context, Stream stream)
    {
        /*
        * Normal successful response content:
            { "choices": [ { ... } ] }
        * Error response (always wrapped in an array):
            [ { "error": { ... } } ]
                → The top‑level value is a JSON array containing a single object.
         */
        using (var doc = await System.Text.Json.JsonDocument.ParseAsync(stream))
        {
            if (doc.RootElement.ValueKind == System.Text.Json.JsonValueKind.Array)
            {
                if (doc.RootElement.GetArrayLength() == 0)
                    throw new InvalidOperationException("Invalid JSON array response (empty array)");

                return context.Sync.ReadForMemory(doc.RootElement[0].ToString(), "response/object");
            }
            return context.Sync.ReadForMemory(doc.RootElement.ToString(), "response/object");
        }
    }

    public override DynamicJsonValue GetAiAttachmentJson(AiAttachment attachment)
    {
        if (attachment.Type == ChatCompletionClient.Constants.AttachmentsRequestFields.MediaTypeApplicationPdf)
            return new DynamicJsonValue
            {
                [ChatCompletionClient.Constants.AttachmentsRequestFields.Type] = ChatCompletionClient.Constants.AttachmentsRequestFields.ImageUrl,
                [ChatCompletionClient.Constants.AttachmentsRequestFields.ImageUrl] = new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.AttachmentsRequestFields.Url] = "data:" + attachment.Type + ";base64," + attachment.Data
                }
            };

        return base.GetAiAttachmentJson(attachment);
    }
}
