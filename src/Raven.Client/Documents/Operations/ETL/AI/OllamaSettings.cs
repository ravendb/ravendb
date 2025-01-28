using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

/// <summary>
/// The configuration for the Ollama API client.
/// </summary>
public sealed class OllamaSettings
{
    public OllamaSettings(string uri, string model)
    {
        Uri = uri;
        Model = model;
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
    /// Returns a value indicating whether the settings are valid.
    /// </summary>
    public bool HasSettings() =>
        string.IsNullOrWhiteSpace(Uri) == false &&
        string.IsNullOrWhiteSpace(Model) == false;

    public DynamicJsonValue ToJson() =>
        new()
        {
            [nameof(Uri)] = Uri,
            [nameof(Model)] = Model
        };
}
