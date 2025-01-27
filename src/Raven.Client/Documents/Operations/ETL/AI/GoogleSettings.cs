#pragma warning disable SKEXP0070
using Microsoft.SemanticKernel.Connectors.Google;
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
#pragma warning restore SKEXP0070
