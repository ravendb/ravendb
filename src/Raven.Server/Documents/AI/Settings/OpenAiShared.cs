using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

// OpenAI code that both wire APIs of the OpenAI connector use: Chat Completions (OpenAiChatCompletionProvider)
// and Responses (OpenAiResponsesProvider). Code moves here when the Responses code needs it; it is never copied.
internal static class OpenAiShared
{
    private const string OpenAiOrganizationHeader = "OpenAI-Organization";
    private const string OpenAiProjectHeader = "OpenAI-Project";

    // The refusal message when the provider blocks content and gives no more detail. Used by every OpenAI-family provider.
    public const string ContentFilterRefusal = "Response blocked due to content policy";

    public static void AddOpenAiHeaders(HttpRequestMessage request, OpenAiSettings settings)
    {
        if (string.IsNullOrEmpty(settings.OrganizationId) == false)
            request.Headers.TryAddWithoutValidation(OpenAiOrganizationHeader, settings.OrganizationId);

        if (string.IsNullOrEmpty(settings.ProjectId) == false)
            request.Headers.TryAddWithoutValidation(OpenAiProjectHeader, settings.ProjectId);
    }

    public static AiError ParseOpenAiError(BlittableJsonReaderObject content, HttpResponseMessage response)
    {
        var error = OpenAiErrorHolder.Deserializer(content).error;

        var errorType = ErrorType.Unknown;
        switch (response.StatusCode)
        {
            case HttpStatusCode.BadRequest:
                if (error.code == "context_length_exceeded" && error.type == "invalid_request_error")
                    errorType = ErrorType.TooManyTokens;
                break;
            case HttpStatusCode.TooManyRequests:
                errorType = error.type switch
                {
                    "insufficient_quota" => ErrorType.InsufficientQuota,
                    "requests" => ErrorType.TooManyRequests,
                    "tokens" => ErrorType.TooManyTokens,
                    _ => ErrorType.Other429
                };
                break;
        }

        return new AiError
        {
            ErrorType = errorType,
            Message = error.message
        };
    }

    public static void AddBearerAuthentication(HttpRequestMessage request, string apiKey)
    {
        request.Headers.Authorization = string.IsNullOrEmpty(apiKey)
            ? null
            : new AuthenticationHeaderValue(ChatCompletionClient.Constants.RequestFields.AuthorizationApiKeyProperty, apiKey);
    }

    public static TimeSpan? GetRetryAfter(HttpResponseMessage response, AiError error)
    {
        var headers = response.Headers;

        if (headers.Contains(ChatCompletionClient.Constants.Headers.RetryAfterMs) == false &&
            headers.Contains(ChatCompletionClient.Constants.Headers.RetryAfter) == false &&
            error.RetryAfter == null)
            return null;

        var retryAfter = error.RetryAfter
                         ?? (ChatCompletionClient.TryGetRetryAfter(response, out var retryAfterFromHeaders) ? retryAfterFromHeaders : TimeSpan.Zero);

        if (headers.TryGetValues(ChatCompletionClient.Constants.Headers.XRateLimitResetTokens, out var resetTokensValues))
        {
            // TPM
            var retryAfterAsString = resetTokensValues.FirstOrDefault();
            if (ChatCompletionClient.TryParseResetTime(retryAfterAsString, out var retryAfterForTokens) == false)
                throw new FormatException($"Unrecognized rate-limit format: '{retryAfterAsString}'");

            retryAfter = retryAfterForTokens > retryAfter ? retryAfterForTokens : retryAfter;
        }

        if (headers.TryGetValues(ChatCompletionClient.Constants.Headers.XRateLimitResetRequests, out var resetRequestsValues))
        {
            // RPM
            var retryAfterAsString = resetRequestsValues.FirstOrDefault();
            if (ChatCompletionClient.TryParseResetTime(retryAfterAsString, out var retryAfterForReqs) == false)
                throw new FormatException($"Unrecognized rate-limit format: '{retryAfterAsString}'");

            retryAfter = retryAfterForReqs > retryAfter ? retryAfterForReqs : retryAfter;
        }

        return retryAfter;
    }

    public static string GetRequestId(HttpResponseHeaders headers)
    {
        if (headers.TryGetValues(ChatCompletionClient.Constants.Headers.XRequestId, out IEnumerable<string> values))
            return values.FirstOrDefault() ?? string.Empty;

        // Azure API Management uses a different header name
        if (headers.TryGetValues("apim-request-id", out values))
            return values.FirstOrDefault() ?? string.Empty;

        return string.Empty;
    }

#pragma warning disable CS0618 // configurations persisted by older clients hold the legacy enum shape
    public static string NormalizeReasoningEffort(string value)
    {
        // ReasoningEffort is free-form, so "3" is a provider value - without this, Enum.TryParse
        // below would read it as the number behind High
        if (int.TryParse(value, out _))
            return value;

        if (Enum.TryParse(value, ignoreCase: false, out OpenAiReasoningEffort effort))
            return effort.ToReasoningEffort();

        return value;
    }
#pragma warning restore CS0618

    private class OpenAiErrorHolder
    {
        public static readonly Func<BlittableJsonReaderObject, OpenAiErrorHolder> Deserializer = JsonDeserializationBase.GenerateJsonDeserializationRoutine<OpenAiErrorHolder>();

        public OpenAiError error { get; set; }
    }

    private class OpenAiError
    {
        public string type { get; set; }
        public string message { get; set; }
        public string code { get; set; }
    }
}
