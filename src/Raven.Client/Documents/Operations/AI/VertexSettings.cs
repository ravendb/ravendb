using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public sealed class VertexSettings : AbstractAiSettings
{
    public VertexSettings()
    {
        // deserialization
    }
    
    public VertexSettings(string model, string apiKey, string location, string projectId, VertexAIVersion? aiVersion = null)
    {
        Model = model;
        ApiKey = apiKey;
        Location = location;
        ProjectId = projectId;
        AiVersion = aiVersion;
    }
    
    /// <summary>
    /// The model ID for the Vertex AI service.
    /// </summary>
    public string Model { get; set; }

    /// <summary>
    /// The API key required for accessing the Vertex AI service.
    /// </summary>
    public string ApiKey { get; set; }
    
    public VertexAIVersion? AiVersion { get; set; }
    
    public string Location { get; set; }
    
    public string ProjectId { get; set; }
    
    public override void ValidateFields(List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(Model))
            errors.Add($"Value of `{nameof(Model)}` field cannot be empty.");
        
        if (string.IsNullOrWhiteSpace(ApiKey))
            errors.Add($"Value of `{nameof(ApiKey)}` field cannot be empty.");
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not VertexSettings vertexSettings)
            return AiSettingsCompareDifferences.All;

        var differences = AiSettingsCompareDifferences.None;

        if (Model != vertexSettings.Model ||
            AiVersion != vertexSettings.AiVersion)
            differences |= AiSettingsCompareDifferences.ModelArchitecture;
        
        if (ApiKey != vertexSettings.ApiKey)
            differences |= AiSettingsCompareDifferences.AuthenticationSettings;

        if (Location != vertexSettings.Location ||
            ProjectId != vertexSettings.ProjectId)
            differences |= AiSettingsCompareDifferences.DeploymentConfiguration;
        
        return differences;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(Model)] = Model;
        json[nameof(ApiKey)] = ApiKey;
        
        if (AiVersion != null)
            json[nameof(AiVersion)] = AiVersion.Value.ToString("G"); // Explicitly convert to string to avoid enum serialization
        
        json[nameof(Location)] = Location;
        json[nameof(ProjectId)] = ProjectId;
        
        return json;
    }
}

/// <summary>
/// Represents the version of the Vertex AI API.
/// </summary>
public enum VertexAIVersion
{
    /// <summary>
    /// Represents the V1 version of the Vertex AI API.
    /// </summary>
    V1,
    
    /// <summary>
    /// Represents the V1 beta version of the Vertex AI API.
    /// </summary>
    V1_Beta
}
