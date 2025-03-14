using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// The configuration for the Ollama API client.
/// </summary>
public sealed class OllamaSettings : AbstractAiSettings
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

    public override void ValidateFields(List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(Uri))
            errors.Add($"Value of `{nameof(Uri)}` field cannot be empty.");

        if (string.IsNullOrWhiteSpace(Model))
            errors.Add($"Value of `{nameof(Model)}` field cannot be empty.");
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

        return differences;
    }

    public override DynamicJsonValue ToJson() =>
        new()
        {
            [nameof(Uri)] = Uri,
            [nameof(Model)] = Model
        };
}
