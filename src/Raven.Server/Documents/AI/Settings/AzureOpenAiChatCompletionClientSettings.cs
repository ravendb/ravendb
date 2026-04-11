using System;
using System.Net;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

internal class AzureOpenAiChatCompletionClientSettings : AbstractOpenAiChatCompletionClientSettings
{
    private new readonly AzureOpenAiSettings _settings;

    private const string ApiVersion = "2024-10-21";

    public AzureOpenAiChatCompletionClientSettings(AzureOpenAiSettings settings)
        : base(settings)
    {
        _settings = settings;
    }

    public override string GetRelativeCompletionUri() => $"openai/deployments/{_settings.DeploymentName}/chat/completions?api-version={ApiVersion}";

    public override string GetRelativeModelsUri() => $"openai/models?api-version={ApiVersion}";
    public override AiError ParseError(BlittableJsonReaderObject content, HttpResponseMessage response)
    {
        var error = AzureOpenAiErrorHolder.Deserializer(content).error;
        ErrorType errorType;
        if (response.StatusCode == HttpStatusCode.TooManyRequests)
        {
            errorType = error.code switch
            {
                "rate_limit_exceeded" or "RateLimitExceeded" => FindErrorFromMessage(error.message),
                "rate_limit_reached" or "RateLimitReached" => FindErrorFromMessage(error.message),
                "insufficient_quota" or "InsufficientQuota" => ErrorType.InsufficientQuota,
                _ => ErrorType.Other429,
            };
        }
        else
        {
            errorType = error.code switch
            {
                "content_filter" or "ContentFilter" => ErrorType.RefusedToAnswer,
                _ => ErrorType.Unknown
            };
        }

        return new AiError
        {
            ErrorType = errorType,
            Message = error.message
        };
    }

    public override string GetRefusal(BlittableJsonReaderObject choice0, BlittableJsonReaderObject message)
    {
        var refusal = base.GetRefusal(choice0, message);
        _ = string.IsNullOrEmpty(refusal)
            && choice0.TryGet(FiltersConstants.ContentFilterResults, out BlittableJsonReaderObject filtersObj)
            && GetFiltersMessage(filtersObj, out refusal);

        return refusal;
    }


    internal static bool GetFiltersMessage(BlittableJsonReaderObject filtersObj, out string message)
    {
        var filtered = false;

        var reasons = filtersObj.GetPropertyNames();

        var sb = new StringBuilder();
        sb.Append("Response blocked due to content policy: ");
        foreach (var reason in reasons)
        {
            if (IsFiltered(filtersObj, reason, out string severity))
            {
                if (filtered)
                    sb.Append(", ");
                sb.Append(reason).Append(" ").Append("(").Append(severity).Append(" severity)");
                filtered = true;
            }
        }
        message = filtered ? sb.ToString() : string.Empty;

        return filtered;
    }

    private static bool IsFiltered(BlittableJsonReaderObject filtersObj, string reason, out string severity)
    {
        // return true if filtered by this reason
        severity = string.Empty;

        if (filtersObj.TryGet(reason, out BlittableJsonReaderObject filterReasonObj) == false)
            return false;

        if (filterReasonObj.TryGet(FiltersConstants.ContentFilterResultFiltered, out bool filtered) == false)
            return false;

        if (filtered == false)
            return false;

        if (filterReasonObj.TryGet(FiltersConstants.ContentFilterResultSeverity, out severity) == false)
        {
            if (filterReasonObj.TryGet(FiltersConstants.ContentFilterResultDetected, out bool detected) && detected)
                severity = FiltersConstants.ContentFilterResultDetected;
            else
                severity = "none";
        }

        return true;
    }

    private static class FiltersConstants
    {
        public const string ContentFilterResults = "content_filter_results";
        public const string ContentFilterResultFiltered = "filtered";
        public const string ContentFilterResultSeverity = "severity";
        public const string ContentFilterResultDetected = "detected";
    }

    private static ErrorType FindErrorFromMessage(string message)
    {
        if(message.Contains("token rate limit"))
            return ErrorType.TooManyTokens;
        if (message.Contains("request rate limit"))
            return ErrorType.TooManyRequests;
        return ErrorType.Other429;
    }

    private class AzureOpenAiErrorHolder
    {
        public static readonly Func<BlittableJsonReaderObject, AzureOpenAiErrorHolder> Deserializer = JsonDeserializationBase.GenerateJsonDeserializationRoutine<AzureOpenAiErrorHolder>();
        public AzureOpenAiError error { get; set; }
    }

    private class AzureOpenAiError
    {
        public string code { get; set; }
        public string message { get; set; }
    }
}
