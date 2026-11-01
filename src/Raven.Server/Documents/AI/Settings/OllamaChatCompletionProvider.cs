using System;
using System.Net;
using System.Net.Http;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

internal class OllamaChatCompletionProvider : AbstractOpenAiCompatibleChatCompletionProvider
{
    private readonly OllamaSettings _settings;

    public OllamaChatCompletionProvider(OllamaSettings settings) 
        : base(settings)
    {
        _settings = settings;
    }

    // Ollama's OpenAI-compatible endpoint silently drops 'tool_choice', so "none" cannot stop tool calls.
    // Instead, the tools array is omitted entirely once tools are disabled.
    public override bool SupportsToolChoiceNone => false;

    // with response_format Ollama constrains the whole output to the schema, so the model can't call tools (ollama#18479, #8095).
    // Keep it after a fix like ollama#18975 ships: older Ollama versions still need the split, and it only costs a fixed one an extra request
    public override bool SupportsToolsWithStructuredOutput => false;

    public override string GetRelativeCompletionUri() => "v1/" + base.GetRelativeCompletionUri();

    public override string GetRelativeModelsUri() => "v1/" + base.GetRelativeModelsUri();
    public override AiError ParseError(BlittableJsonReaderObject content, HttpResponseMessage response)
    {
        // Ollama does not provide structured error responses, so we return the raw content as the error message.
        return new AiError
        {
            ErrorType = ErrorType.Unknown,
            Message = content.ToString()
        };
    }

    public override void HandleCompletionRequestPayload(AsyncBlittableJsonTextWriter writer)
    {
        if (_settings.Think.HasValue)
        {
            writer.WriteComma();
            writer.WritePropertyName(Wire.RequestFields.Think);
            writer.WriteBool(_settings.Think.Value);
        }

        if (_settings.Temperature.HasValue)
        {
            writer.WriteComma();
            writer.WritePropertyName(Wire.RequestFields.Temperature);
            writer.WriteDouble(_settings.Temperature.Value);
        }
    }
}
