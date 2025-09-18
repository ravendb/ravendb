using System;
using System.Collections.Generic;
using System.IO;
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

    public string GoogleCredentialsJson { get; set; }

    public VertexAIVersion? AiVersion { get; set; }
    
    public string Location { get; set; }

    public string GetProjectId()
    {
        var credentialJsonType = JObject.Parse(GoogleCredentialsJson);
        if (credentialJsonType.TryGetValue(ProjectIdPropertyName, StringComparison.OrdinalIgnoreCase, out var projectIdValue))
            return projectIdValue.Value<string>();
        
        throw new InvalidDataException($"Couldn't find {nameof(ProjectIdPropertyName)} in the provided {nameof(GoogleCredentialsJson)}.");
    }

    public override void ValidateFields(List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(Model))
            errors.Add($"Value of `{nameof(Model)}` field cannot be empty.");
        
        if (string.IsNullOrWhiteSpace(GoogleCredentialsJson))
            errors.Add($"Value of `{nameof(GoogleCredentialsJson)}` field cannot be empty.");
        
        var credentialJsonType = JObject.Parse(GoogleCredentialsJson);
        if (credentialJsonType.TryGetValue(ProjectIdPropertyName, StringComparison.OrdinalIgnoreCase, out var projectIdValue) == false)
            errors.Add($"`{nameof(GoogleCredentialsJson)}` has to contain `{ProjectIdPropertyName}` property.");
        
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

        if (Location != vertexSettings.Location)
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
