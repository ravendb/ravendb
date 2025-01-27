using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public abstract class OpenAiBaseSettings : AbstractAiSettings
{
    /// <summary>
    /// The API key to used to authenticate with the service.
    /// </summary>
    public string ApiKey { get; set; }

    /// <summary>
    /// The service endpoint that the client will send requests to. If not set, the default endpoint will be used.
    /// </summary>
    public string Endpoint { get; set; }

    /// <summary>
    /// The model that should be used.
    /// </summary>
    public string Model { get; set; }

    public override bool HasSettings() =>
        string.IsNullOrWhiteSpace(ApiKey) == false &&
        string.IsNullOrWhiteSpace(Model) == false &&
        string.IsNullOrWhiteSpace(Endpoint) == false;

    public override DynamicJsonValue ToJson() =>
        new()
        {
            [nameof(ApiKey)] = ApiKey,
            [nameof(Endpoint)] = Endpoint,
            [nameof(Model)] = Model
        };
}
