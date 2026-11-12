using System.Net.Http;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

internal class OllamaChatCompletionProvider : AbstractOpenAiChatCompletionProvider
{
    private new readonly OllamaSettings _settings;

    public OllamaChatCompletionProvider(OllamaSettings settings)
        : base(settings)
    {
        _settings = settings;
    }

    // Ollama's OpenAI-compatible endpoint silently drops 'tool_choice', so "none" cannot stop tool calls.
    // Instead, the tools array is omitted entirely once tools are disabled.
    public override bool SupportsToolChoiceNone => false;

    // with response_format Ollama can't call tools (ollama#18479, #8095); keep it after a fix ships, older versions still need it
    public override bool SupportsToolsWithStructuredOutput => false;

    public override string GetRelativeCompletionUri() => OllamaShared.V1Prefix + base.GetRelativeCompletionUri();

    public override string GetRelativeModelsUri() => OllamaShared.V1Prefix + base.GetRelativeModelsUri();

    public override AiError ParseError(BlittableJsonReaderObject content, HttpResponseMessage response) => OllamaShared.ParseOllamaError(content);

    public override void HandleCompletionRequestPayload(AsyncBlittableJsonTextWriter writer)
    {
        base.HandleCompletionRequestPayload(writer);

        if (_settings.Think.HasValue)
        {
            writer.WriteComma();
            writer.WritePropertyName(Wire.RequestFields.Think);
            writer.WriteBool(_settings.Think.Value);
        }

        if (string.IsNullOrWhiteSpace(_settings.ReasoningEffort) == false)
        {
            writer.WriteComma();
            writer.WritePropertyName(Wire.RequestFields.ReasoningEffort);
            writer.WriteString(OpenAiShared.NormalizeReasoningEffort(_settings.ReasoningEffort.Trim()));
        }
    }
}
