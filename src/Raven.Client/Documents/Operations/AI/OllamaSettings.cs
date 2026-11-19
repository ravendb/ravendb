using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// The configuration for the Ollama API client.
/// </summary>
public sealed class OllamaSettings : OpenAiChatApiSettings
{
    /// <summary>
    /// Initializes a new instance of <see cref="OllamaSettings"/> with the specified URI and model.
    /// </summary>
    /// <param name="uri">The URI of the Ollama API.</param>
    /// <param name="model">The model to be used.</param>
    public OllamaSettings(string uri, string model) : base(apiKey: null, endpoint: uri, model: model)
    {
    }

    public OllamaSettings()
    {
        // deserialization
    }

    /// <summary>
    /// The URI of the Ollama API (the <see cref="OpenAiBaseSettings.Endpoint"/>); stored as <c>"Uri"</c>.
    /// </summary>
    public string Uri
    {
        get => Endpoint;
        set => Endpoint = value;
    }

    /// <summary>
    /// Controls whether thinking models engage their reasoning process before responding.
    /// When true, thinking models will perform their internal reasoning process (uses more tokens, slower, better quality for complex tasks).
    /// When false, thinking models skip the reasoning process and respond directly (fewer tokens, faster, may reduce quality for complex reasoning).
    /// When null, the parameter is not sent (backwards compatible).
    /// Disable thinking for speed/efficiency in simple tasks, enable for complex tasks requiring reasoning.
    /// </summary>
    public bool? Think { get; set; } = null;

    // existing Ollama connection strings keep Chat Completions
    internal override OpenAiChatApi GetDefaultChatApi() => OpenAiChatApi.ChatCompletions;

    // No API key rule: local Ollama deployments usually have no key. When set, it is sent as a Bearer token on chat
    // requests (for an Ollama behind an authenticating proxy); embeddings requests do not send it.
    public override void ValidateFields(List<string> errors)
    {
        CoreValidateFields(errors);

        if (string.IsNullOrWhiteSpace(Uri))
            errors.Add($"Value of `{nameof(Uri)}` field cannot be empty.");

        // Ollama embeddings are created from Uri and Model only (AiExtensions.cs), so a value would be ignored
        if (Dimensions.HasValue)
            errors.Add($"`{nameof(Dimensions)}` is not supported for Ollama.");
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not OllamaSettings ollamaSettings)
            return AiSettingsCompareDifferences.All;

        var differences = base.Compare(other);

        if (Think != ollamaSettings.Think)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        return differences;
    }

    /// <summary>
    /// Serializes the settings to JSON structure.
    /// </summary>
    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(Uri)] = Uri; // the name older versions read
        json[nameof(Think)] = Think;

        return json;
    }

    // No trailing '/' is added: the provider appends "v1/..." to the address as it is stored.
    public override Uri GetBaseEndpointUri() => new Uri(Endpoint);
}
