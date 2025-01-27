#pragma warning disable SKEXP0070
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class GoogleSettings
{
    /// <summary>The model that should be used.</summary>
    public string Model { get; set; }

    /// <summary>The API key to used to authenticate with the service.</summary>
    public string ApiKey { get; set; }

    /// <summary>  The version of the Google API. Defaults to <see href="GoogleAIVersion.V1_Beta"/>.</summary>
    public GoogleAIVersion ApiVersion { get; set; } = GoogleAIVersion.V1_Beta;

    /// <summary>The optional service ID.</summary>
    /// <remarks>
    /// The service ID is an optional identifier that can be used to distinguish between different instances of the same service.
    /// </remarks>
    public string ServiceId { get; set; }

    public DynamicJsonValue ToJson() =>
        new()
        {
            [nameof(Model)] = Model,
            [nameof(ApiKey)] = ApiKey,
            [nameof(ApiVersion)] = ApiVersion.ToString(),
            [nameof(ServiceId)] = ServiceId
        };
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
