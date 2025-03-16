using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public abstract class OpenAiBaseSettings : AbstractAiSettings
{
    protected OpenAiBaseSettings(string apiKey, string endpoint, string model, int? dimensions = null)

    {
        ApiKey = apiKey;
        Endpoint = endpoint;
        Model = model;
        Dimensions = dimensions;
    }

    protected OpenAiBaseSettings()
    {
        // deserialization
    }

    /// <summary>
    /// The API key to use to authenticate with the service.
    /// </summary>
    public string ApiKey { get; set; }

    /// <summary>
    /// The service endpoint that the client will send requests to.
    /// </summary>
    public string Endpoint { get; set; }

    /// <summary>
    /// The model that should be used.
    /// </summary>
    public string Model { get; set; }

    /// <summary>
    /// The number of dimensions that the model should use.
    /// </summary>
    public int? Dimensions { get; set; }

    public override void ValidateFields(List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(ApiKey))
            errors.Add($"Value of `{nameof(ApiKey)}` field cannot be empty.");

        if (string.IsNullOrWhiteSpace(Endpoint))
            errors.Add($"Value of `{nameof(Endpoint)}` field cannot be empty.");

        if (string.IsNullOrWhiteSpace(Model))
            errors.Add($"Value of `{nameof(Model)}` field cannot be empty.");

        if (Dimensions is <= 0)
            errors.Add($"Value of `{nameof(Dimensions)}` field must be positive.");
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not OpenAiBaseSettings openAiSettings)
            return AiSettingsCompareDifferences.All;

        var differences = AiSettingsCompareDifferences.None;

        if (ApiKey != openAiSettings.ApiKey)
            differences |= AiSettingsCompareDifferences.AuthenticationSettings;

        if (Endpoint != openAiSettings.Endpoint)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        if (Model != openAiSettings.Model)
            differences |= AiSettingsCompareDifferences.ModelArchitecture;

        if (Dimensions != openAiSettings.Dimensions)
            differences |= AiSettingsCompareDifferences.EmbeddingDimensions;

        return differences;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(Model)] = Model;
        json[nameof(ApiKey)] = ApiKey;

        if (string.IsNullOrWhiteSpace(Endpoint) == false)
            json[nameof(Endpoint)] = Endpoint;

        if (Dimensions.HasValue)
            json[nameof(Dimensions)] = Dimensions.Value;

        return json;
    }
}
