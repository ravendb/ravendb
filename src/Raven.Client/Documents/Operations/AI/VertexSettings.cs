using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public sealed class VertexSettings : AbstractAiSettings
{
    public VertexSettings()
    {
        // deserialization
    }
    
    public VertexSettings(string model, string googleCredentialsJson, string location, VertexAIVersion? aiVersion = null)
    {
        Model = model;
        GoogleCredentialsJson = googleCredentialsJson;
        Location = location;
        AiVersion = aiVersion;
    }
    
    private const string ProjectIdPropertyName = "project_id";
    
    /// <summary>
    /// The model ID for the Vertex AI service.
    /// </summary>
    public string Model { get; set; }

    public string GoogleCredentialsJson
    {
        get => _googleCredentialsJson;
        set
        {
            _googleCredentialsJson = value;

            var credentialJsonType = JObject.Parse(value);
            if (credentialJsonType.TryGetValue(ProjectIdPropertyName, StringComparison.OrdinalIgnoreCase, out var projectIdValue))
                ProjectId = projectIdValue.Value<string>();
        }
    }
    
    private string _googleCredentialsJson;

    public VertexAIVersion? AiVersion { get; set; }
    
    public string Location { get; set; }
    
    internal string ProjectId { get; set; }

    public override void ValidateFields(List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(Model))
            errors.Add($"Value of `{nameof(Model)}` field cannot be empty.");
        
        if (string.IsNullOrWhiteSpace(GoogleCredentialsJson))
            errors.Add($"Value of `{nameof(GoogleCredentialsJson)}` field cannot be empty.");
        
        if (string.IsNullOrWhiteSpace(Location))
            errors.Add($"Value of `{nameof(Location)}` field cannot be empty.");
    }

    public override AiSettingsCompareDifferences Compare(AbstractAiSettings other)
    {
        if (other is not VertexSettings vertexSettings)
            return AiSettingsCompareDifferences.All;

        var differences = AiSettingsCompareDifferences.None;

        if (Model != vertexSettings.Model ||
            AiVersion != vertexSettings.AiVersion)
            differences |= AiSettingsCompareDifferences.ModelArchitecture;
        
        if (GoogleCredentialsJson != vertexSettings.GoogleCredentialsJson)
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
        json[nameof(GoogleCredentialsJson)] = GoogleCredentialsJson;
        
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
