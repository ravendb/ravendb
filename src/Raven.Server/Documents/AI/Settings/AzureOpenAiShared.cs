using System;
using System.Net;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

// Azure OpenAI code that both wire APIs of the Azure connector use: Chat Completions (AzureOpenAiChatCompletionProvider)
// and Responses (AzureOpenAiResponsesProvider). Code moves here when the Responses code needs it; it is never copied.
internal static class AzureOpenAiShared
{
    public const string ChatCompletionsApiVersion = "2024-10-21";

    public static string ModelsUri => $"openai/models?api-version={ChatCompletionsApiVersion}";

    public static AiError ParseAzureError(BlittableJsonReaderObject content, HttpResponseMessage response)
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

    public static bool GetFiltersMessage(BlittableJsonReaderObject filtersObj, out string message)
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

    // Responses API: Azure reports content filtering in a top-level content_filters[], one entry per source_type
    // ("prompt", "completion"). Returns null when no entry is blocked. A blocked entry always contributes text, even when
    // its content_filter_results flags no category, so blocked content is never returned as an answer.
    public static string GetBlockedContentFilterMessage(BlittableJsonReaderObject response)
    {
        if (response.TryGet(FiltersConstants.ContentFilters, out BlittableJsonReaderArray entries) == false || entries == null)
            return null;

        StringBuilder sb = null;
        foreach (var item in entries)
        {
            if (item is not BlittableJsonReaderObject entry)
                continue;

            if (entry.TryGet(FiltersConstants.Blocked, out bool blocked) == false || blocked == false)
                continue;

            string message;
            if (entry.TryGet(FiltersConstants.ContentFilterResults, out BlittableJsonReaderObject results) == false || results == null ||
                GetFiltersMessage(results, out message) == false)
                message = "Response blocked due to content policy";

            if (entry.TryGet(FiltersConstants.SourceType, out string sourceType) == false || string.IsNullOrEmpty(sourceType))
                sourceType = "content";

            if (sb == null)
                sb = new StringBuilder();
            else
                sb.Append("; ");

            sb.Append(sourceType).Append(": ").Append(message);
        }

        return sb?.ToString();
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

    internal static class FiltersConstants
    {
        public const string ContentFilterResults = "content_filter_results";
        public const string ContentFilterResultFiltered = "filtered";
        public const string ContentFilterResultSeverity = "severity";
        public const string ContentFilterResultDetected = "detected";
        public const string ContentFilters = "content_filters";
        public const string Blocked = "blocked";
        public const string SourceType = "source_type";
    }

    private static ErrorType FindErrorFromMessage(string message)
    {
        if (message.Contains("token rate limit"))
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
