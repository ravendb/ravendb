using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

internal class OllamaChatCompletionClientSettings : AbstractChatCompletionClientSettings
{
    private readonly OllamaSettings _settings;

    public OllamaChatCompletionClientSettings(OllamaSettings settings) 
        : base(settings.Uri, apiKey: null, settings.Model)
    {
        _settings = settings;
    }

    public override void HandleCompletionRequestPayload(AsyncBlittableJsonTextWriter writer)
    {
        if (_settings.Think.HasValue)
        {
            writer.WriteComma();
            writer.WritePropertyName(Constants.RequestFields.Think);
            writer.WriteBool(_settings.Think.Value);
        }

        if (_settings.Temperature.HasValue)
        {
            writer.WriteComma();
            writer.WritePropertyName(Constants.RequestFields.Temperature);
            writer.WriteDouble(_settings.Temperature.Value);
        }
    }
}
