#nullable enable
using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public abstract class OpenAiBaseSettings : AbstractAiSettings
{
    protected OpenAiBaseSettings(string apiKey, string endpoint, string model)
    {
        ApiKey = apiKey;
        Endpoint = endpoint;
        Model = model;
    }

    protected OpenAiBaseSettings()
    {
        // deserialization
    }

    /// <summary>
    /// The API key to used to authenticate with the service.
    /// </summary>
    public string ApiKey { get; set; }

    /// <summary>
    /// The service endpoint that the client will send requests to.
    /// </summary>
    public string Endpoint { get; set; }

    /// <summary>
    /// The model that should be used.
    /// </summary>
    public string Model { get; set; }

    public override bool HasSettings()
    {
        return string.IsNullOrWhiteSpace(ApiKey) == false &&
               string.IsNullOrWhiteSpace(Endpoint) == false &&
               string.IsNullOrWhiteSpace(Model) == false;
    }

    public override bool HasCriticalChanges(AbstractAiSettings other)
    {
        if (other is not OpenAiBaseSettings openAiBaseSettings)
            return true;

        return Model != openAiBaseSettings.Model;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(ApiKey)] = ApiKey,
            [nameof(Model)] = Model
        };

        if (string.IsNullOrWhiteSpace(Endpoint) == false)
            json[nameof(Endpoint)] = Endpoint;

        return json;
    }
}
