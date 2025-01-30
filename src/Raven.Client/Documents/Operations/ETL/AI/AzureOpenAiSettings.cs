#nullable enable
using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class AzureOpenAiSettings : OpenAiBaseSettings
{
    public AzureOpenAiSettings(string apiKey, string endpoint, string model, string deploymentName, int? dimensions = null) : base(apiKey, endpoint, model)
    {
        DeploymentName = deploymentName;
        Dimensions = dimensions;
    }
    
    public AzureOpenAiSettings()
    {
        // deserialization
    }
    
    /// <summary>AzureOpenAI deployment name.
    /// <see href="https://learn.microsoft.com/azure/cognitive-services/openai/how-to/create-resource">Learn more</see>
    /// </summary>
    public string DeploymentName { get; set; }

    /// <summary>The number of dimensions the resulting output embeddings should have.</summary>
    /// <remarks>Only supported in "text-embedding-3" and later models.</remarks>
    public int? Dimensions { get; set; }

    public override bool HasSettings()
    {
        return base.HasSettings() &&
               string.IsNullOrWhiteSpace(DeploymentName) == false;
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not AzureOpenAiSettings azureSettings)
            return AiSettingsCompareDifferences.All;

        var differences = base.Compare(other);

        if (DeploymentName != azureSettings.DeploymentName)
            differences |= AiSettingsCompareDifferences.DeploymentConfiguration;

        if (Dimensions != azureSettings.Dimensions)
            differences |= AiSettingsCompareDifferences.EmbeddingDimensions;

        return differences;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(DeploymentName)] = DeploymentName;

        if (Dimensions.HasValue)
            json[nameof(Dimensions)] = Dimensions;
        
        return json;
    }
}
