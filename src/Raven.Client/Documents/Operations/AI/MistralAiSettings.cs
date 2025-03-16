using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public sealed class MistralAiSettings : AbstractAiSettings
{
    public MistralAiSettings()
    {
        // deserialization
    }

    public MistralAiSettings(string model, string apiKey, string endpoint)
    {
        Model = model;
        Endpoint = endpoint;
        ApiKey = apiKey;
    }

    /// <summary>
    /// The model ID for the Mistral AI service.
    /// </summary>
    public string Model { get; set; }

    /// <summary>
    /// The endpoint for the Mistral AI service.
    /// </summary>
    public string Endpoint { get; set; }

    /// <summary>
    /// The API key required for accessing the Mistral AI service.
    /// </summary>
    public string ApiKey { get; set; }

    public override void ValidateFields(List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(Model))
            errors.Add($"Value of `{nameof(Model)}` field cannot be empty.");

        if (string.IsNullOrWhiteSpace(Endpoint))
            errors.Add($"Value of `{nameof(Endpoint)}` field cannot be empty.");

        if (string.IsNullOrWhiteSpace(ApiKey))
            errors.Add($"Value of `{nameof(ApiKey)}` field cannot be empty.");
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not MistralAiSettings mistralAiSettings)
            return AiSettingsCompareDifferences.All;

        var differences = AiSettingsCompareDifferences.None;

        if (Model != mistralAiSettings.Model)
            differences |= AiSettingsCompareDifferences.ModelArchitecture;

        if (Endpoint != mistralAiSettings.Endpoint)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        if (ApiKey != mistralAiSettings.ApiKey)
            differences |= AiSettingsCompareDifferences.AuthenticationSettings;

        return differences;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(Model)] = Model;
        json[nameof(ApiKey)] = ApiKey;
        json[nameof(Endpoint)] = Endpoint;
        return json;
    }
}
