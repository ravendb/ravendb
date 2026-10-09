using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

internal abstract class AbstractOpenAiChatCompletionProvider : AbstractOpenAiCompatibleChatCompletionProvider
{
    protected readonly OpenAiBaseSettings _settings;

    public override bool EnablePromptCaching => _settings.EnablePromptCache ?? true;

    protected AbstractOpenAiChatCompletionProvider(OpenAiBaseSettings settings)
        : base(settings)
    {
        _settings = settings;
    }

    public override void HandleCompletionRequestPayload(AsyncBlittableJsonTextWriter writer)
    {
        if (_settings.Temperature.HasValue)
        {
            writer.WriteComma();
            writer.WritePropertyName(Wire.RequestFields.Temperature);
            writer.WriteDouble(_settings.Temperature.Value);
        }
    }
}
