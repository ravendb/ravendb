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
    private new readonly OpenAiSettings _settings;
    private readonly bool _disableReasoning;
    private const string GptModelPrefix = "gpt-";
    private static readonly Version MinVersionRequiringReasoningDisabled = new(5, 4);

    public OpenAiChatCompletionClientSettings(OpenAiSettings settings)
        : base(settings)
    {
        _settings = settings;
        _disableReasoning = ShouldDisableReasoningForChatCompletions(settings.Model);
    }

    // gpt-5.4 and later reject function tools combined with reasoning on /v1/chat/completions:
    private static bool ShouldDisableReasoningForChatCompletions(string model)
    {
        if (string.IsNullOrWhiteSpace(model))
            return false;

        var modelName = model.AsSpan().Trim();

        // e.g. openai/gpt-5.6-sol
        var lastSlash = modelName.LastIndexOf('/');
        if (lastSlash >= 0)
            modelName = modelName[(lastSlash + 1)..];

        if (modelName.StartsWith(GptModelPrefix, StringComparison.OrdinalIgnoreCase) == false)
            return false;

        var versionText = modelName[GptModelPrefix.Length..];

        // e.g. "5.6-sol" -> "5.6"
        var suffixIndex = versionText.IndexOf('-');
        if (suffixIndex >= 0)
            versionText = versionText[..suffixIndex];

        // Version requires at least major.minor. A major-only name is accepted only as a single
        // digit, so a future "gpt-6" follows the policy while "gpt-35-turbo" (Azure's alias for
        // gpt-3.5-turbo, seen on OpenAI-compatible providers such as LiteLLM-fronted deployments)
        // is not treated as version 35.
        if (versionText.IndexOf('.') < 0)
        {
            return versionText.Length == 1 &&
                   char.IsAsciiDigit(versionText[0]) &&
                   versionText[0] > '5';
        }

        return Version.TryParse(versionText, out var version) &&
               version >= MinVersionRequiringReasoningDisabled;
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
        var reasoningEffort = GetEffectiveReasoningEffort();
        if (reasoningEffort != null)
        {
            writer.WriteComma();
            writer.WritePropertyName(Constants.RequestFields.ReasoningEffort);
            writer.WriteString(reasoningEffort);
        }
        if (_settings.Seed.HasValue)
        {
            // Use a fixed seed to make sampling more reproducible across runs.
            // This helps stabilize tests. Combined with a low reasoning_effort
            // it further reduces the probability of flaky responses.
            writer.WriteComma();
            writer.WritePropertyName(Constants.RequestFields.Seed);
            writer.WriteInteger(_settings.Seed.Value);
        }
        base.HandleCompletionRequestPayload(writer);
    }

    // null means the field is omitted from the request
    private string GetEffectiveReasoningEffort()
    {
        if (_disableReasoning)
            return Constants.RequestFields.ReasoningEffortNoneValue;

        var reasoningEffort = _settings.ReasoningEffort;
        if (string.IsNullOrWhiteSpace(reasoningEffort))
            return null;

        return NormalizeReasoningEffort(reasoningEffort.Trim());
    }

#pragma warning disable CS0618 // configurations persisted by older clients hold the legacy enum shape
    private static string NormalizeReasoningEffort(string value)
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
