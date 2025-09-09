using System;
using System.Collections.Generic;
using Sparrow.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// The configuration for the Ollama API client.
/// </summary>
public sealed class OllamaSettings : AbstractAiSettings, IChatCompletionSettings
{
    public OllamaSettings(string uri, string model)
    {
        Uri = uri;
        Model = model;
    }

    public OllamaSettings()
    {
        // deserialization
    }

    /// <summary>
    /// The URI of the Ollama API.
    /// </summary>
    public string Uri { get; set; }

    /// <summary>
    /// The model that should be used.
    /// </summary>
    public string Model { get; set; }

    /// <summary>
    /// Controls whether thinking models engage their reasoning process before responding.
    /// When true, thinking models will perform their internal reasoning process (uses more tokens, slower, better quality for complex tasks).
    /// When false, thinking models skip the reasoning process and respond directly (fewer tokens, faster, may reduce quality for complex reasoning).
    /// When null, the parameter is not sent (backwards compatible).
    /// Disable thinking for speed/efficiency in simple tasks, enable for complex tasks requiring reasoning.
    /// </summary>
    public bool? Think { get; set; } = null;

    /// <summary>
    /// Controls randomness of the model output. Range typically [0.0, 2.0].
    /// Higher values (e.g., 1.0+) make output more creative and diverse; lower values (e.g., 0.2) make it more deterministic.
    /// When null, the parameter is not sent.
    /// </summary>
    public double? Temperature { get; set; } = null;

    public override void ValidateFields(List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(Uri))
            errors.Add($"Value of `{nameof(Uri)}` field cannot be empty.");

        if (string.IsNullOrWhiteSpace(Model))
            errors.Add($"Value of `{nameof(Model)}` field cannot be empty.");
        
        if (Temperature is < 0d)
            errors.Add($"Value of `{nameof(Temperature)}` field must be non-negative.");
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not OllamaSettings ollamaSettings)
            return AiSettingsCompareDifferences.All;

        var differences = AiSettingsCompareDifferences.None;

        if (Model != ollamaSettings.Model)
            differences |= AiSettingsCompareDifferences.ModelArchitecture;

        if (Uri != ollamaSettings.Uri)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        if (Think != ollamaSettings.Think)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;
        
        if (Temperature.HasValue != ollamaSettings.Temperature.HasValue ||
            (Temperature.HasValue && ollamaSettings.Temperature.HasValue && Temperature.Value.AlmostEquals(ollamaSettings.Temperature.Value) == false))
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        return differences;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(Model)] = Model;
        json[nameof(Uri)] = Uri;
        json[nameof(Think)] = Think;
        json[nameof(Temperature)] = Temperature;

        return json;
    }

    public string ApiKey => null;
    public string Endpoint => Uri;
    public Uri GetBaseEndpointUri() => new Uri(Endpoint);
}
