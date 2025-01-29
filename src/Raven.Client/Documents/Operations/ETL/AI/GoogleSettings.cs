#pragma warning disable SKEXP0070
#nullable enable
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

    /// <summary>The model that should be used.</summary>
    public string Model { get; set; }

    /// <summary>The API key to used to authenticate with the service.</summary>
    public string ApiKey { get; set; }

    /// <summary>The version of the Google AI.</summary>
    public GoogleAIVersion? AiVersion { get; set; }

    public override bool HasSettings()
    {
        return string.IsNullOrWhiteSpace(Model) == false &&
               string.IsNullOrWhiteSpace(ApiKey) == false;
    }

    public override bool HasCriticalChanges(AbstractAiSettings other)
    {
        if (other is not GoogleSettings googleSettings)
            return true;

        return Model != googleSettings.Model ||
               AiVersion != googleSettings.AiVersion;
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
