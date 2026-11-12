using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// The configuration for the Ollama API client.
/// </summary>
public sealed class OllamaSettings : OpenAiBaseSettings
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
    /// The URI of the Ollama API. Same value as <see cref="OpenAiBaseSettings.Endpoint"/>, kept under this name
    /// because it is the name Ollama connection strings are stored with: it is stored as <c>"Uri"</c>.
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
    /// On Ollama's <c>/v1/chat/completions</c>, <c>think: false</c> may not stop reasoning; setting
    /// <see cref="OpenAiBaseSettings.ReasoningEffort"/> to <c>none</c> does (observed on Ollama 0.40.16).
    /// </summary>
    public bool? Think { get; set; } = null;

    /// <summary>
    /// Optional. Local Ollama deployments usually have no API key. When set, it is sent as a Bearer token
    /// on chat requests (for an Ollama behind an authenticating proxy); embeddings requests do not send it.
    /// </summary>
    public override string ApiKey
    {
        get => base.ApiKey;
        set => base.ApiKey = value;
    }

    /// <summary>
    /// Not supported for Ollama: a value is a validation error.
    /// </summary>
    public override int? Dimensions
    {
        get => base.Dimensions;
        set => base.Dimensions = value;
    }

    // Existing Ollama connection strings keep using Chat Completions unless ApiMode is set.
    internal override OpenAiApiMode GetApiMode() => ApiMode ?? OpenAiApiMode.ChatCompletions;

    // Ollama validates its own fields rather than the OpenAI-family rules, which require an API key.
    // The rules and messages are the ones Ollama had before it joined OpenAiBaseSettings, plus Dimensions.
    public override void ValidateFields(List<string> errors)
    {
        if (EmbeddingsMaxConcurrentBatches is <= 0)
            errors.Add($"Value of `{nameof(EmbeddingsMaxConcurrentBatches)}` field must be a positive number.");

        if (string.IsNullOrWhiteSpace(Uri))
            errors.Add($"Value of `{nameof(Uri)}` field cannot be empty.");

        if (string.IsNullOrWhiteSpace(Model))
            errors.Add($"Value of `{nameof(Model)}` field cannot be empty.");

        if (Temperature is < 0d)
            errors.Add($"Value of `{nameof(Temperature)}` field must be non-negative.");

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

        // The address is stored as "Uri", and the API key only when set, so older readers see the same keys as before.
        json.RemoveInMemoryPropertyByName(nameof(Endpoint));
        if (string.IsNullOrWhiteSpace(ApiKey))
            json.RemoveInMemoryPropertyByName(nameof(ApiKey));

        // Written for Ollama even when null, as before; the base writes it only when set.
        json.RemoveInMemoryPropertyByName(nameof(Temperature));

        json[nameof(Uri)] = Uri;
        json[nameof(Think)] = Think;
        json[nameof(Temperature)] = Temperature;

        return json;
    }

    // No trailing '/' is added: the provider appends "v1/..." to the address as it is stored.
    public override Uri GetBaseEndpointUri() => new Uri(Endpoint);
}
