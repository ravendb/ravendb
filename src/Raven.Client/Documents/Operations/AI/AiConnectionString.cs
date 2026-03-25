using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Represents a connection string for AI providers used by RavenDB features
/// like text embeddings generation and chat completions. Exactly one concrete
/// provider settings object should be configured (e.g. OpenAI, Azure OpenAI,
/// Ollama, Google, Hugging Face, Mistral, or Embedded).
/// </summary>
public sealed class AiConnectionString : ConnectionString
{
    /// <summary>
    /// A user-defined identifier for AI tasks associated with this connection string.
    /// This is used when generating and reusing embeddings and other AI-related operations.
    /// </summary>
    public string Identifier { get; set; }

    /// <summary>
    /// OpenAI provider configuration. Only one provider settings object may be set.
    /// </summary>
    public OpenAiSettings OpenAiSettings { get; set; }

    /// <summary>
    /// Azure OpenAI provider configuration. Only one provider settings object may be set.
    /// </summary>
    public AzureOpenAiSettings AzureOpenAiSettings { get; set; }

    /// <summary>
    /// Ollama provider configuration. Only one provider settings object may be set.
    /// </summary>
    public OllamaSettings OllamaSettings { get; set; }

    /// <summary>
    /// Embedded (server-side ONNX) provider configuration. Only one provider settings object may be set.
    /// </summary>
    public EmbeddedSettings EmbeddedSettings { get; set; }

    /// <summary>
    /// Google AI provider configuration. Only one provider settings object may be set.
    /// </summary>
    public GoogleSettings GoogleSettings { get; set; }

    /// <summary>
    /// Hugging Face provider configuration. Only one provider settings object may be set.
    /// </summary>
    public HuggingFaceSettings HuggingFaceSettings { get; set; }

    /// <summary>
    /// Mistral AI provider configuration. Only one provider settings object may be set.
    /// </summary>
    public MistralAiSettings MistralAiSettings { get; set; }

    /// <summary>
    /// Vertex AI provider configuration. Only one provider settings object may be set.
    /// </summary>
    public VertexSettings VertexSettings { get; set; }

    /// <summary>
    /// The connection string type. Always <see cref="ConnectionStringType.Ai"/> for this class.
    /// </summary>
    public override ConnectionStringType Type => ConnectionStringType.Ai;

    /// <summary>
    /// The AI model category used with this connection (e.g., embeddings or chat).
    /// </summary>
    public AiModelType ModelType { get; set; }

    protected override void ValidateImpl(List<string> errors)
    {
        var allSettings = new AbstractAiSettings[]
        {
            OpenAiSettings,
            AzureOpenAiSettings,
            OllamaSettings,
            EmbeddedSettings,
            GoogleSettings,
            HuggingFaceSettings,
            MistralAiSettings,
            VertexSettings
        };

        var configuredSettings = allSettings.Where(s => s != null).ToArray();

        foreach (var setting in configuredSettings)
            setting.ValidateFields(errors);

        switch (configuredSettings.Length)
        {
            case 0:
                var allSettingsNames = allSettings.Select(s => s.GetType().Name);
                errors.Add($"At least one of the following settings must be set: {string.Join(", ", allSettingsNames)}");
                break;
            case > 1:
                var configuredSettingsNames = configuredSettings.Select(s => s.GetType().Name);
                errors.Add($"Only one of the following settings can be set: {string.Join(", ", configuredSettingsNames)}");
                break;
        }
    }

    internal string GenerateIdentifier() => EmbeddingsGenerationConfiguration.GenerateIdentifier(Name);

    internal bool ValidateIdentifier(out List<string> errors)
    {
        return AiTaskIdentifierHelper.ValidateIdentifier(Identifier, out errors);
    }

    /// <summary>
    /// Compares this connection string with another and returns a set of flags describing
    /// which aspects differ (e.g., model, endpoint, authentication). This can be used to
    /// determine whether embeddings should be regenerated or configuration updated.
    /// </summary>
    /// <param name="newConnectionString">The connection string to compare with.</param>
    /// <returns>A set of <see cref="AiSettingsCompareDifferences"/> flags describing differences.</returns>
    public AiSettingsCompareDifferences Compare(AiConnectionString newConnectionString)
    {
        if (newConnectionString == null)
            return AiSettingsCompareDifferences.All;

        var result = AiSettingsCompareDifferences.None;

        if (Identifier != newConnectionString.Identifier)
            result |= AiSettingsCompareDifferences.Identifier;

        if (ModelType != newConnectionString.ModelType)
            result |= AiSettingsCompareDifferences.ModelArchitecture;

        var oldProvider = GetActiveProvider();
        var newProvider = newConnectionString.GetActiveProvider();

        if (oldProvider != newProvider)
            return AiSettingsCompareDifferences.All;

        result |= oldProvider switch
        {
            AiConnectorType.OpenAi => OpenAiSettings.Compare(newConnectionString.OpenAiSettings),
            AiConnectorType.AzureOpenAi => AzureOpenAiSettings.Compare(newConnectionString.AzureOpenAiSettings),
            AiConnectorType.Ollama => OllamaSettings.Compare(newConnectionString.OllamaSettings),
            AiConnectorType.Embedded => EmbeddedSettings.Compare(newConnectionString.EmbeddedSettings),
            AiConnectorType.Google => GoogleSettings.Compare(newConnectionString.GoogleSettings),
            AiConnectorType.HuggingFace => HuggingFaceSettings.Compare(newConnectionString.HuggingFaceSettings),
            AiConnectorType.MistralAi => MistralAiSettings.Compare(newConnectionString.MistralAiSettings),
            AiConnectorType.Vertex => VertexSettings.Compare(newConnectionString.VertexSettings),
            _ => AiSettingsCompareDifferences.All
        };

        return result;
    }

    internal bool UsingEncryptedCommunicationChannel()
    {
        AiConnectorType aiConnectorType = GetActiveProvider();
        switch (aiConnectorType)
        {
            case AiConnectorType.Ollama:
                return OllamaSettings.Uri.StartsWith("https");
            case AiConnectorType.OpenAi:
                return this.OpenAiSettings.Endpoint.StartsWith("https");
            case AiConnectorType.AzureOpenAi:
                return this.AzureOpenAiSettings.Endpoint.StartsWith("https");
            case AiConnectorType.MistralAi:
                return this.MistralAiSettings.Endpoint.StartsWith("https");
            case AiConnectorType.HuggingFace:
                // Endpoint is optional for HuggingFace, it will use the default endpoint if not provided, which is HTTPS
                return string.IsNullOrWhiteSpace(this.HuggingFaceSettings.Endpoint) || this.HuggingFaceSettings.Endpoint.StartsWith("https");
            case AiConnectorType.Embedded:
            case AiConnectorType.Google:
            case AiConnectorType.Vertex:
                return true;

            default:
                throw new NotSupportedException($"Unknown AI connector type: {aiConnectorType}");
        }
    }

    /// <summary>
    /// Returns the active AI provider based on which settings object is configured.
    /// </summary>
    /// <returns>The active <see cref="AiConnectorType"/> or <see cref="AiConnectorType.None"/> if none set.</returns>
    public AiConnectorType GetActiveProvider()
    {
        if (OpenAiSettings != null)
            return AiConnectorType.OpenAi;
        if (AzureOpenAiSettings != null)
            return AiConnectorType.AzureOpenAi;
        if (OllamaSettings != null)
            return AiConnectorType.Ollama;
        if (EmbeddedSettings != null)
            return AiConnectorType.Embedded;
        if (GoogleSettings != null)
            return AiConnectorType.Google;
        if (HuggingFaceSettings != null)
            return AiConnectorType.HuggingFace;
        if (MistralAiSettings != null)
            return AiConnectorType.MistralAi;
        if (VertexSettings != null)
            return AiConnectorType.Vertex;

        return AiConnectorType.None;
    }

    /// <summary>
    /// Determines whether the specified <see cref="ConnectionString"/> is equal to the current instance,
    /// including the active provider and its settings.
    /// </summary>
    /// <param name="connectionString">The other connection string to compare.</param>
    /// <returns><c>true</c> if equal; otherwise, <c>false</c>.</returns>
    public override bool IsEqual(ConnectionString connectionString)
    {
        if (base.IsEqual(connectionString) == false)
            return false;

        if (connectionString is not AiConnectionString aiConnectionString)
            return false;

        if (Identifier != aiConnectionString.Identifier)
            return false;

        if (ModelType != aiConnectionString.ModelType) 
            return false;

        var activeProvider = GetActiveProvider();
        var otherActiveProvider = aiConnectionString.GetActiveProvider();

        if (activeProvider != otherActiveProvider)
            return false;

        return activeProvider switch
        {
            AiConnectorType.OpenAi => OpenAiSettings.Compare(aiConnectionString.OpenAiSettings) == AiSettingsCompareDifferences.None,
            AiConnectorType.AzureOpenAi => AzureOpenAiSettings.Compare(aiConnectionString.AzureOpenAiSettings) == AiSettingsCompareDifferences.None,
            AiConnectorType.Ollama => OllamaSettings.Compare(aiConnectionString.OllamaSettings) == AiSettingsCompareDifferences.None,
            AiConnectorType.Embedded => EmbeddedSettings.Compare(aiConnectionString.EmbeddedSettings) == AiSettingsCompareDifferences.None,
            AiConnectorType.Google => GoogleSettings.Compare(aiConnectionString.GoogleSettings) == AiSettingsCompareDifferences.None,
            AiConnectorType.HuggingFace => HuggingFaceSettings.Compare(aiConnectionString.HuggingFaceSettings) == AiSettingsCompareDifferences.None,
            AiConnectorType.MistralAi => MistralAiSettings.Compare(aiConnectionString.MistralAiSettings) == AiSettingsCompareDifferences.None,
            AiConnectorType.Vertex => VertexSettings.Compare(aiConnectionString.VertexSettings) == AiSettingsCompareDifferences.None,
            AiConnectorType.None => true,
            _ => false
        };
    }

    /// <summary>
    /// Converts the connection string and its configured provider settings to a JSON representation.
    /// </summary>
    /// <returns>A <see cref="DynamicJsonValue"/> containing the serialized configuration.</returns>
    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(Identifier)] = Identifier;
        json[nameof(ModelType)] = ModelType;
        json[nameof(OpenAiSettings)] = OpenAiSettings?.ToJson();
        json[nameof(AzureOpenAiSettings)] = AzureOpenAiSettings?.ToJson();
        json[nameof(OllamaSettings)] = OllamaSettings?.ToJson();
        json[nameof(EmbeddedSettings)] = EmbeddedSettings?.ToJson();
        json[nameof(GoogleSettings)] = GoogleSettings?.ToJson();
        json[nameof(HuggingFaceSettings)] = HuggingFaceSettings?.ToJson();
        json[nameof(MistralAiSettings)] = MistralAiSettings?.ToJson();
        json[nameof(VertexSettings)] = VertexSettings?.ToJson();

        return json;
    }

    internal int GetQueryEmbeddingsMaxConcurrentBatches(int globalQueryEmbeddingsMaxConcurrentBatches)
    {
        var provider = GetActiveProviderInstance(); 
        return provider?.EmbeddingsMaxConcurrentBatches ?? globalQueryEmbeddingsMaxConcurrentBatches;
    }

    internal AbstractAiSettings GetActiveProviderInstance()
    {
        return OpenAiSettings ??
               AzureOpenAiSettings ??
               OllamaSettings ??
               EmbeddedSettings ??
               GoogleSettings ??
               HuggingFaceSettings ??
               VertexSettings ??
               (AbstractAiSettings)MistralAiSettings;
    }
}
