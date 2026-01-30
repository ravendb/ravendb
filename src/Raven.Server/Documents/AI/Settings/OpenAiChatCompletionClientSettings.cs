using System;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

internal class OpenAiChatCompletionClientSettings : AbstractOpenAiChatCompletionClientSettings
{
    private readonly OpenAiSettings _settings;
    public OpenAiChatCompletionClientSettings(OpenAiSettings settings) 
        : base(settings)
    {
        _settings = settings;
    }

    public override void AddHeaders(HttpRequestMessage request)
    {
        if (string.IsNullOrEmpty(_settings.OrganizationId) == false)
            request.Headers.TryAddWithoutValidation(Constants.Headers.OpenAiOrganization, _settings.OrganizationId);

        if (string.IsNullOrEmpty(_settings.ProjectId) == false)
            request.Headers.TryAddWithoutValidation(Constants.Headers.OpenAiProject, _settings.ProjectId);
    }

    public override AiError ParseError(BlittableJsonReaderObject content, HttpResponseMessage response)
    {
        var error = OpenAiErrorHolder.Deserializer(content).error;

        var errorType = ErrorType.Unknown;
        if (response.StatusCode == HttpStatusCode.TooManyRequests)
        {
            errorType = error.type switch
            {
                "insufficient_quota" => ErrorType.InsufficientQuota,
                "requests" => ErrorType.TooManyRequests,
                "tokens" => ErrorType.TooManyTokens,
                _ => ErrorType.Other429
            };
        }

        return new AiError
        {
            ErrorType = errorType,
            Message = error.message
        };
    }

    private class OpenAiErrorHolder
    {
        public static readonly Func<BlittableJsonReaderObject, OpenAiErrorHolder> Deserializer = JsonDeserializationBase.GenerateJsonDeserializationRoutine<OpenAiErrorHolder>();

        public OpenAiError error { get; set; }
    }

    private class OpenAiError
    {
        public string type { get; set; }
        public string message { get; set; }
    }
}
