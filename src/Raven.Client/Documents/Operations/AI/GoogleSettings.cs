#pragma warning disable SKEXP0070
using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public sealed class GoogleSettings : OpenAiBaseSettings
{
    public GoogleSettings(string model, string apiKey, string endpoint = null, GoogleAIVersion? aiVersion = null, int? dimensions = null, double? temperature = null) :
        base(apiKey, endpoint, model, dimensions, temperature)
    {
        AiVersion = aiVersion;
    }

    public GoogleSettings()
    {
        // deserialization
    }

    /// <summary>The version of Google AI to use.</summary>
    public GoogleAIVersion? AiVersion { get; set; }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not GoogleSettings googleSettings)
            return AiSettingsCompareDifferences.All;

        var differences = base.Compare(other);

        if (AiVersion != googleSettings.AiVersion)
            differences |= AiSettingsCompareDifferences.ModelArchitecture;

        return differences;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        if (AiVersion != null)
            json[nameof(AiVersion)] = AiVersion.Value.ToString("G"); // Explicitly convert to string to avoid enum serialization

        return json;
    }

    public override Uri GetBaseEndpointUri()
    {
        if (string.IsNullOrEmpty(Endpoint) == false)
            return base.GetBaseEndpointUri();

        return new Uri("https://generativelanguage.googleapis.com/");
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
