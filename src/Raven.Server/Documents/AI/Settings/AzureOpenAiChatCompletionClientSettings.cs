using System;
using System.Net;
using System.Net.Http;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

internal class AzureOpenAiChatCompletionClientSettings : AbstractOpenAiChatCompletionClientSettings
{
    private readonly AzureOpenAiSettings _settings;

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
