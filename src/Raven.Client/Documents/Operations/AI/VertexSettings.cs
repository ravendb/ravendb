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
    
    /// <summary>
    /// Initializes a new instance of <see cref="VertexSettings"/> with the specified model, credentials, and location.
    /// </summary>
    /// <param name="model">The model ID for the Vertex AI service.</param>
    /// <param name="googleCredentialsJson">The JSON string containing the Google service account credentials.</param>
    /// <param name="location">The Google Cloud region where the model is deployed.</param>
    /// <param name="aiVersion">The optional API version of Vertex AI.</param>
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

    /// <summary>
    /// The JSON string containing the Google service account credentials.
    /// </summary>
    public string GoogleCredentialsJson { get; set; }

    /// <summary>
    /// The specific API version of Vertex AI to use.
    /// </summary>
    public VertexAIVersion? AiVersion { get; set; }
    
    /// <summary>
    /// The Google Cloud region (location) where the model is deployed (e.g., "us-central1").
    /// </summary>
    public string Location { get; set; }

    internal string GetProjectId()
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
        
        var projectId = GetProjectId();
        if (string.IsNullOrWhiteSpace(projectId))
            errors.Add($"Value of `{ProjectIdPropertyName}` field in `{nameof(GoogleCredentialsJson)}` cannot be empty.");
        
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

    /// <summary>
    /// Serializes the settings to JSON structure.
    /// </summary>
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
