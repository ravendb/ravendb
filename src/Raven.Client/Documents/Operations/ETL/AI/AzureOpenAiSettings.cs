using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class AzureOpenAiSettings : OpenAiBaseSettings
{
    /// <summary>AzureOpenAI deployment name.
    /// <see cref="https://learn.microsoft.com/azure/cognitive-services/openai/how-to/create-resource">Learn more</see>
    /// </summary>
    public string DeploymentName { get; set; }

    /// <summary>A local identifier for given AI service</summary>
    public string ServiceId { get; set; }

    /// <summary>The number of dimensions the resulting output embeddings should have.</summary>
    /// <remarks>Only supported in "text-embedding-3" and later models.</remarks>
    public int Dimensions { get; set; }

    public override bool HasSettings() => 
        base.HasSettings() && 
        string.IsNullOrWhiteSpace(DeploymentName) == false;

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        
        json[nameof(DeploymentName)] = DeploymentName;
        json[nameof(ServiceId)] = ServiceId;
        json[nameof(Dimensions)] = Dimensions;
        
        return json;
    }
}
