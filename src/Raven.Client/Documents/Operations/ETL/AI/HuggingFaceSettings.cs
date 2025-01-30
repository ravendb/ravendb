using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class HuggingFaceSettings : AbstractAiSettings
{
    public HuggingFaceSettings(string model, string endpoint = null, string apiKey = null)
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
    public string Endpoint { get; set; }

    /// <summary>
    /// The API key required for accessing the Hugging Face service.
    /// </summary>
    public string ApiKey { get; set; }

    public override bool HasSettings()
    {
        return string.IsNullOrWhiteSpace(Model) == false &&
               string.IsNullOrWhiteSpace(ApiKey) == false;
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not HuggingFaceSettings huggingFaceSettings)
            return AiSettingsCompareDifferences.All;

        var differences = AiSettingsCompareDifferences.None;

        if (Model != huggingFaceSettings.Model)
            differences |= AiSettingsCompareDifferences.ModelArchitecture;

        if (Endpoint != huggingFaceSettings.Endpoint)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        if (ApiKey != huggingFaceSettings.ApiKey)
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

        if (string.IsNullOrWhiteSpace(Endpoint) == false)
            json[nameof(Endpoint)] = Endpoint;

        return json;
    }
}
