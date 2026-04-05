using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

internal abstract class AbstractOpenAiChatCompletionClientSettings : AbstractChatCompletionClientSettings
{
    protected readonly OpenAiBaseSettings _settings;

    public override bool EnablePromptCaching => _settings.EnablePromptCache ?? true;

    protected AbstractOpenAiChatCompletionClientSettings(OpenAiBaseSettings settings)
        : base(settings)
    {
        _settings = settings;
    }

    public override void HandleCompletionRequestPayload(AsyncBlittableJsonTextWriter writer)
    {
        if (_settings.Temperature.HasValue)
        {
            writer.WriteComma();
            writer.WritePropertyName(Constants.RequestFields.Temperature);
            writer.WriteDouble(_settings.Temperature.Value);
        }
    }
}
