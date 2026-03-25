using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public sealed class HuggingFaceSettings : AbstractAiSettings
{
    /// <summary>
    /// Initializes a new instance of <see cref="HuggingFaceSettings"/> with the specified API key, model, and optional endpoint.
    /// </summary>
    /// <param name="apiKey">The API key required for accessing the Hugging Face service.</param>
    /// <param name="model">The name of the Hugging Face model.</param>
    /// <param name="endpoint">The optional endpoint for the service.</param>
    public HuggingFaceSettings(string apiKey, string model, string endpoint = null)
    {
        Model = model;
        Endpoint = endpoint;
        ApiKey = apiKey;
    }

    /// <summary>
    /// Initializes a new instance of <see cref="HuggingFaceSettings"/>.
    /// </summary>
    public HuggingFaceSettings()
    {
        // deserialization
    }

    /// <summary>
    /// The name of the Hugging Face model.
    /// </summary>
    public string Model { get; set; }

    /// <summary>
    /// The endpoint for the service. If not specified, the default endpoint will be used.
    /// </summary>
    public string Endpoint { get; set; }

    /// <summary>
    /// The API key required for accessing the Hugging Face service.
    /// </summary>
    public string ApiKey { get; set; }

    public override void ValidateFields(List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(Model))
            errors.Add($"Value of `{nameof(Model)}` field cannot be empty.");

        if (string.IsNullOrWhiteSpace(ApiKey))
            errors.Add($"Value of `{nameof(ApiKey)}` field cannot be empty.");
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not HuggingFaceSettings huggingFaceSettings)
            return AiSettingsCompareDifferences.All;

        var differences = AiSettingsCompareDifferences.None;

        if (Model != huggingFaceSettings.Model)
            differences |= AiSettingsCompareDifferences.ModelArchitecture;

        if (Endpoint != huggingFaceSettings.Endpoint)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        if (ApiKey != huggingFaceSettings.ApiKey)
            differences |= AiSettingsCompareDifferences.AuthenticationSettings;

        return differences;
    }

    /// <summary>
    /// Converts the settings into a JSON representation.
    /// </summary>
    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(Model)] = Model;
        json[nameof(ApiKey)] = ApiKey;

        if (string.IsNullOrWhiteSpace(Endpoint) == false)
            json[nameof(Endpoint)] = Endpoint;

        return json;
    }
}
