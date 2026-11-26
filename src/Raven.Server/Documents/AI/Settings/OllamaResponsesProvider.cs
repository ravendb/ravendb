using System.Net.Http;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI.Settings;

// Ollama over its OpenAI-compatible Responses endpoint (/v1/responses). "think" is Ollama's own field.
internal sealed class OllamaResponsesProvider : AbstractOpenAiResponsesProvider
{
    private readonly OllamaSettings _settings;

    public OllamaResponsesProvider(OllamaSettings settings)
        : base(settings)
    {
        _settings = settings;
    }

    // Same reason as OllamaChatCompletionProvider: Ollama drops 'tool_choice', so the tools are omitted when disabled.
    public override bool SupportsToolChoiceNone => false;

    public override bool SupportsToolsWithStructuredOutput => false;

    public override string GetRelativeCompletionUri() => OllamaShared.V1Prefix + base.GetRelativeCompletionUri();

    public override string GetRelativeModelsUri() => OllamaShared.V1Prefix + base.GetRelativeModelsUri();

    public override AiError ParseError(BlittableJsonReaderObject content, HttpResponseMessage response) => OllamaShared.ParseOllamaError(content);

    protected override void HandleCompletionRequestPayload(DynamicJsonValue body)
    {
        base.HandleCompletionRequestPayload(body);

        if (_settings.Think.HasValue)
            body[Wire.Think] = _settings.Think.Value;
    }
}
