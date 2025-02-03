#pragma warning disable SKEXP0070
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class GoogleSettings : AbstractAiSettings
{
    public GoogleSettings(string model, string apiKey, GoogleAIVersion? aiVersion = null)
    {
        Model = model;
        ApiKey = apiKey;
        AiVersion = aiVersion;
    }

    public GoogleSettings()
    {
        // deserialization
    }

    /// <summary>The model that should be used.</summary>
    public string Model { get; set; }

    /// <summary>The API key to used to authenticate with the service.</summary>
    public string ApiKey { get; set; }

    /// <summary>The version of the Google AI.</summary>
    public GoogleAIVersion? AiVersion { get; set; }

    public override void ValidateMandatoryFields(ref List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(Model))
            errors.Add($"Value of `{nameof(Model)}` field cannot be empty");

        if (string.IsNullOrWhiteSpace(ApiKey))
            errors.Add($"Value of `{nameof(ApiKey)}` field cannot be empty");
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
            json[nameof(AiVersion)] = AiVersion.ToString();

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
