#pragma warning disable SKEXP0070
using System;
using System.Collections.Generic;
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

    // Google has only Chat Completions
    /// <summary>
    /// Not supported for Google: a value is a validation error (Google only has Chat Completions).
    /// </summary>
    public override OpenAiApiMode? ApiMode
    {
        get => base.ApiMode;
        set => base.ApiMode = value;
    }

    /// <summary>
    /// Not supported for Google: a value is a validation error.
    /// </summary>
    public override string ReasoningEffort
    {
        get => base.ReasoningEffort;
        set => base.ReasoningEffort = value;
    }

    public override void ValidateFields(List<string> errors)
    {
        base.ValidateFields(errors);

        if (ApiMode.HasValue)
            errors.Add($"`{nameof(ApiMode)}` is not supported for Google: Google only has the Chat Completions API.");

        if (string.IsNullOrWhiteSpace(ReasoningEffort) == false)
            errors.Add($"`{nameof(ReasoningEffort)}` is not supported for Google.");
    }

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
