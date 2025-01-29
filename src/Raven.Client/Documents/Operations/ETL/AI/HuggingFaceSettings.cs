#nullable enable
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class HuggingFaceSettings : AbstractAiSettings
{
    public HuggingFaceSettings(string model, string? endpoint = null, string? apiKey = null, string? serviceId = null)
    {
        Model = model;
        Endpoint = endpoint;
        ApiKey = apiKey;
    }

    public HuggingFaceSettings()
    {
        // deserialization
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

    public override bool HasSettings()
    {
        return string.IsNullOrWhiteSpace(Model) == false;
    }

    public override bool HasCriticalChanges(AbstractAiSettings other)
    {
        if (other is not HuggingFaceSettings huggingFaceSettings)
            return true;

        return Model != huggingFaceSettings.Model;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Model)] = Model
        };

        if (string.IsNullOrWhiteSpace(Endpoint) == false)
            json[nameof(Endpoint)] = Endpoint;

        if (string.IsNullOrWhiteSpace(ApiKey) == false)
            json[nameof(ApiKey)] = ApiKey;

        return json;
    }

}
