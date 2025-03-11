#pragma warning disable SKEXP0070
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public sealed class GoogleSettings : AbstractAiSettings
{
    public GoogleSettings(string model, string apiKey, GoogleAIVersion? aiVersion = null, int? dimensions = null)
    {
        Model = model;
        ApiKey = apiKey;
        AiVersion = aiVersion;
        Dimensions = dimensions;
    }

    public GoogleSettings()
    {
        // deserialization
    }

    /// <summary>The model that should be used.</summary>
    public string Model { get; set; }

    /// <summary>The API key to use to authenticate with the service.</summary>
    public string ApiKey { get; set; }

    /// <summary>The version of Google AI to use.</summary>
    public GoogleAIVersion? AiVersion { get; set; }

    /// <summary>
    /// The number of dimensions that the model should use.
    /// </summary>
    public int? Dimensions { get; set; }

    public override void ValidateMandatoryFields(ref List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(Model))
            errors.Add($"Value of `{nameof(Model)}` field cannot be empty.");

        if (string.IsNullOrWhiteSpace(ApiKey))
            errors.Add($"Value of `{nameof(ApiKey)}` field cannot be empty.");

        if (Dimensions is <= 0)
            errors.Add($"Value of `{nameof(Dimensions)}` field must be positive.");
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not GoogleSettings googleSettings)
            return AiSettingsCompareDifferences.All;

        var differences = AiSettingsCompareDifferences.None;

        if (Model != googleSettings.Model ||
            AiVersion != googleSettings.AiVersion)
            differences |= AiSettingsCompareDifferences.ModelArchitecture;

        if (ApiKey != googleSettings.ApiKey)
            differences |= AiSettingsCompareDifferences.AuthenticationSettings;

        if (Dimensions != googleSettings.Dimensions)
            differences |= AiSettingsCompareDifferences.EmbeddingDimensions;

        return differences;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Model)] = Model,
            [nameof(ApiKey)] = ApiKey
        };

        if (AiVersion != null)
            json[nameof(AiVersion)] = AiVersion.Value.ToString("G"); // Explicitly convert to string to avoid enum serialization

        if (Dimensions.HasValue)
            json[nameof(Dimensions)] = Dimensions.Value;

        return json;
    }
}

/// <summary>
/// Represents the version of the Google AI API.
/// </summary>
public enum GoogleAIVersion
{
    /// <summary>
    /// Represents the V1 version of the Google AI API.
    /// </summary>
    V1,

    /// <summary>
    /// Represents the V1-beta version of the Google AI API.
    /// </summary>
    V1_Beta
}

#pragma warning restore SKEXP0070
