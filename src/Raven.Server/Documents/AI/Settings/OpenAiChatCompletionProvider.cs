using System;
using System.Net.Http;
using System.Text.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

internal class OpenAiChatCompletionProvider : AbstractOpenAiChatCompletionProvider
{
    private new readonly OpenAiSettings _settings;
    private readonly bool _disableReasoning;
    private const string GptModelPrefix = "gpt-";
    private static readonly Version MinVersionRequiringReasoningDisabled = new(5, 4);

    public OpenAiChatCompletionProvider(OpenAiSettings settings)
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

    public override void AddHeaders(HttpRequestMessage request) => OpenAiShared.AddOpenAiHeaders(request, _settings);

    public override AiError ParseError(BlittableJsonReaderObject content, HttpResponseMessage response) => OpenAiShared.ParseOpenAiError(content, response);

    public override void HandleCompletionRequestPayload(AsyncBlittableJsonTextWriter writer)
    {
        var reasoningEffort = GetEffectiveReasoningEffort();
        if (reasoningEffort != null)
        {
            writer.WriteComma();
            writer.WritePropertyName(Wire.RequestFields.ReasoningEffort);
            writer.WriteString(reasoningEffort);
        }
        if (_settings.Seed.HasValue)
        {
            // Use a fixed seed to make sampling more reproducible across runs.
            // This helps stabilize tests. Combined with a low reasoning_effort
            // it further reduces the probability of flaky responses.
            writer.WriteComma();
            writer.WritePropertyName(Wire.RequestFields.Seed);
            writer.WriteInteger(_settings.Seed.Value);
        }
        base.HandleCompletionRequestPayload(writer);
    }

    // null means the field is omitted from the request
    private string GetEffectiveReasoningEffort()
    {
        if (_disableReasoning)
            return Wire.RequestFields.ReasoningEffortNoneValue;

        var reasoningEffort = _settings.ReasoningEffort;
        if (string.IsNullOrWhiteSpace(reasoningEffort))
            return null;

        return OpenAiShared.NormalizeReasoningEffort(reasoningEffort.Trim());
    }
}
