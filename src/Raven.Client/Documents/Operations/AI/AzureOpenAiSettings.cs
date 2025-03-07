using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

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

    /// <summary>Azure OpenAI deployment name.
    /// <see href="https://learn.microsoft.com/azure/cognitive-services/openai/how-to/create-resource">Learn more</see>
    /// </summary>
    public string DeploymentName { get; set; }

    public override void ValidateMandatoryFields(ref List<string> errors)
    {
        base.ValidateMandatoryFields(ref errors);

        if (string.IsNullOrWhiteSpace(DeploymentName))
            errors.Add($"Value for `{nameof(DeploymentName)}` field cannot be empty.");
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
