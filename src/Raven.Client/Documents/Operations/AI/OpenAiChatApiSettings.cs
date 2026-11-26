using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Base configuration for the connectors whose chat requests can go to either OpenAI chat API: OpenAI, Azure OpenAI and Ollama.
/// </summary>
public abstract class OpenAiChatApiSettings : OpenAiBaseSettings
{
    protected OpenAiChatApiSettings(string apiKey, string endpoint, string model, int? dimensions = null, double? temperature = null)
        : base(apiKey, endpoint, model, dimensions, temperature)
    {
    }

    protected OpenAiChatApiSettings()
    {
        // deserialization
    }

    /// <summary>
    /// The API that chat requests are sent to. When not set, the connector's default is used.
    /// </summary>
    public OpenAiChatApiType? ChatApi { get; set; }

    /// <summary>
    /// The reasoning effort for models that support it, e.g. <c>none</c>, <c>low</c>, <c>medium</c> or <c>high</c>. Sent as is.
    /// </summary>
    public string ReasoningEffort { get; set; }

    internal OpenAiChatApiType GetChatApi() => ChatApi ?? GetDefaultChatApi();

    internal abstract OpenAiChatApiType GetDefaultChatApi();

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not OpenAiChatApiSettings chatApiSettings)
            return AiSettingsCompareDifferences.All;

        var differences = base.Compare(other);

        if (ChatApi != chatApiSettings.ChatApi || ReasoningEffort != chatApiSettings.ReasoningEffort)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        return differences;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        if (ChatApi.HasValue)
            json[nameof(ChatApi)] = ChatApi.Value;

        if (string.IsNullOrWhiteSpace(ReasoningEffort) == false)
            json[nameof(ReasoningEffort)] = ReasoningEffort;

        return json;
    }
}
