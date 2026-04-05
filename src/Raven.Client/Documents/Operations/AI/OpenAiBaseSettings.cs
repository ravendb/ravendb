using System;
using System.Collections.Generic;
using Sparrow.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Base configuration for OpenAI-compatible providers (OpenAI, Azure OpenAI),
/// including common fields like API key, endpoint, model and embedding dimensions.
/// </summary>
public abstract class OpenAiBaseSettings : AbstractAiSettings, IAiSettings
{
    protected OpenAiBaseSettings(string apiKey, string endpoint, string model, int? dimensions = null, double? temperature = null)
    {
        ApiKey = apiKey;
        Endpoint = endpoint;
        Model = model;
        Dimensions = dimensions;
        Temperature = temperature;
    }

    protected OpenAiBaseSettings()
    {
        // deserialization
    }

    /// <summary>
    /// The API key to use to authenticate with the service.
    /// </summary>
    public string ApiKey { get; set; }

    /// <summary>
    /// The service endpoint that the client will send requests to.
    /// </summary>
    public string Endpoint { get; set; }

    public virtual Uri GetBaseEndpointUri()
    {
        var endpoint = Endpoint;
        if (endpoint.EndsWith("/") == false)
            endpoint += "/";

        return new Uri(endpoint);
    }

    /// <summary>
    /// The model that should be used.
    /// </summary>
    public string Model { get; set; }


    /// <summary>
    /// The number of dimensions that the model should use.
    /// </summary>
    public int? Dimensions { get; set; }

    /// <summary>
    /// Controls randomness of the model output. Range typically [0.0, 2.0].
    /// Higher values (e.g., 1.0+) make output more creative and diverse; lower values (e.g., 0.2) make it more deterministic.
    /// When null, the parameter is not sent.
    /// </summary>
    public double? Temperature { get; set; } = null;

    /// <summary>
    /// Enables sending a <c>prompt_cache_key</c> field in chat completion requests,
    /// allowing providers that support it to cache and reuse prompt prefixes across
    /// requests with the same key.
    /// When <c>null</c>, the server applies a provider-specific default
    /// </summary>
    public bool? EnablePromptCache { get; set; }

    public override void ValidateFields(List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(ApiKey))
            errors.Add($"Value of `{nameof(ApiKey)}` field cannot be empty.");

        if (string.IsNullOrWhiteSpace(Model))
            errors.Add($"Value of `{nameof(Model)}` field cannot be empty.");

        if (Dimensions is <= 0)
            errors.Add($"Value of `{nameof(Dimensions)}` field must be positive.");

        if (Temperature is < 0d)
            errors.Add($"Value of `{nameof(Temperature)}` field must be non-negative.");
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not OpenAiBaseSettings openAiSettings)
            return AiSettingsCompareDifferences.All;

        var differences = AiSettingsCompareDifferences.None;

        if (ApiKey != openAiSettings.ApiKey)
            differences |= AiSettingsCompareDifferences.AuthenticationSettings;

        if (Endpoint != openAiSettings.Endpoint)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        if (Model != openAiSettings.Model)
            differences |= AiSettingsCompareDifferences.ModelArchitecture;

        if (Dimensions != openAiSettings.Dimensions)
            differences |= AiSettingsCompareDifferences.EmbeddingDimensions;

        if (Temperature.HasValue != openAiSettings.Temperature.HasValue ||
            (Temperature.HasValue && openAiSettings.Temperature.HasValue && Temperature.Value.AlmostEquals(openAiSettings.Temperature.Value) == false))
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        if (EnablePromptCache != openAiSettings.EnablePromptCache)
            differences |= AiSettingsCompareDifferences.EndpointConfiguration;

        return differences;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(Model)] = Model;
        json[nameof(ApiKey)] = ApiKey;

        if (string.IsNullOrWhiteSpace(Endpoint) == false)
            json[nameof(Endpoint)] = Endpoint;

        if (Dimensions.HasValue)
            json[nameof(Dimensions)] = Dimensions.Value;

        if (Temperature.HasValue)
            json[nameof(Temperature)] = Temperature;

        if (EnablePromptCache.HasValue)
            json[nameof(EnablePromptCache)] = EnablePromptCache.Value;

        return json;
    }
}
