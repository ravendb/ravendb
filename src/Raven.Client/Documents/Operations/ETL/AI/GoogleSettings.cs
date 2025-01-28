#pragma warning disable SKEXP0070
#nullable enable
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class GoogleSettings
{
    public GoogleSettings(string model, string apiKey, GoogleAIVersion? apiVersion = null, string? serviceId = null)
    {
        Model = model;
        ApiKey = apiKey;
        ApiVersion = apiVersion;
        ServiceId = serviceId;
    }

    /// <summary>The model that should be used.</summary>
    public string Model { get; set; }

    /// <summary>The API key to used to authenticate with the service.</summary>
    public string ApiKey { get; set; }

    /// <summary>The version of the Google API.</summary>
    public GoogleAIVersion? ApiVersion { get; set; }

    /// <summary>The optional service ID.</summary>
    /// <remarks>
    /// The service ID is an optional identifier that can be used to distinguish between different instances of the same service.
    /// </remarks>
    public string? ServiceId { get; set; }

    public bool HasSettings() =>
        string.IsNullOrWhiteSpace(Model) == false &&
        string.IsNullOrWhiteSpace(ApiKey) == false;

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Model)] = Model,
            [nameof(ApiKey)] = ApiKey
        };

        if (string.IsNullOrWhiteSpace(ServiceId) == false)
            json[nameof(ServiceId)] = ServiceId;

        if (ApiVersion != null)
            json[nameof(ApiVersion)] = ApiVersion.ToString();

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

public sealed class HuggingFaceSettings
{
    public HuggingFaceSettings(string model, string? endpoint = null, string? apiKey = null, string? serviceId = null)
    {
        Model = model;
        Endpoint = endpoint;
        ApiKey = apiKey;
        ServiceId = serviceId;
    }

    /// <summary>
    /// The name of the Hugging Face model.
    /// </summary>
    public string Model { get; set; }

    /// <summary>
    /// The endpoint for the text embedding generation service. If not specified, the default endpoint will be used.
    /// </summary>
    public string? Endpoint { get; set; }

    /// <summary>
    /// The API key required for accessing the Hugging Face service.
    /// </summary>
    public string? ApiKey { get; set; }

    /// <summary>
    /// A local identifier for the given AI service.
    /// </summary>
    public string? ServiceId { get; set; }

    public bool HasSettings() =>
        string.IsNullOrWhiteSpace(Model) == false;

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Model)] = Model
        };

        if (string.IsNullOrWhiteSpace(Endpoint) == false)
            json[nameof(Endpoint)] = Endpoint;

        if (string.IsNullOrWhiteSpace(ApiKey) == false)
            json[nameof(ApiKey)] = ApiKey;

        if (string.IsNullOrWhiteSpace(ServiceId) == false)
            json[nameof(ServiceId)] = ServiceId;

        return json;
    }

}
#pragma warning restore SKEXP0070
