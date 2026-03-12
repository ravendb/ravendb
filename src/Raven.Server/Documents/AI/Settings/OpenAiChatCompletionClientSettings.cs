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

    public override void HandleCompletionRequestPayload(AsyncBlittableJsonTextWriter writer)
    {
        if (_settings.ReasoningEffort.HasValue)
        {
            writer.WriteComma();
            writer.WritePropertyName("reasoning_effort");
            writer.WriteString(_settings.ReasoningEffort.Value.ToString().ToLower());
        }
        if (_settings.Seed.HasValue)
        {
            // Use a fixed seed to make sampling more reproducible across runs.
            // This helps stabilize tests. Combined with reasoning_effort="minimal"
            // it further reduces the probability of flaky responses.
            writer.WriteComma();
            writer.WritePropertyName("seed");
            writer.WriteInteger(_settings.Seed.Value);
        }
        base.HandleCompletionRequestPayload(writer);
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
        public string code { get; set; }
    }
}
