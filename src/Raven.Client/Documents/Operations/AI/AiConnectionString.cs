using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public sealed class AiConnectionString : ConnectionString
{
    public string Identifier { get; set; }

    public OpenAiSettings OpenAiSettings { get; set; }

    public AzureOpenAiSettings AzureOpenAiSettings { get; set; }

    public OllamaSettings OllamaSettings { get; set; }

    public EmbeddedSettings EmbeddedSettings { get; set; }

    public GoogleSettings GoogleSettings { get; set; }

    public HuggingFaceSettings HuggingFaceSettings { get; set; }

    public MistralAiSettings MistralAiSettings { get; set; }

    public override ConnectionStringType Type => ConnectionStringType.Ai;

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
            MistralAiSettings
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
        return EmbeddingsGenerationConfiguration.ValidateIdentifier(Identifier, out errors);
    }

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
                return true;

            default:
                throw new NotSupportedException($"Unknown AI connector type: {aiConnectorType}");
        }
    }

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

        return AiConnectorType.None;
    }

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
            AiConnectorType.None => true,
            _ => false
        };
    }

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

        return json;
    }

    internal int GetQueryEmbeddingsMaxConcurrentBatches(int globalQueryEmbeddingsMaxConcurrentBatches)
    {
        var provider = GetActiveProviderInstance(); 
        return provider?.EmbeddingsMaxConcurrentBatches ?? globalQueryEmbeddingsMaxConcurrentBatches;
    }

    private AbstractAiSettings GetActiveProviderInstance()
    {
        return OpenAiSettings ??
               AzureOpenAiSettings ??
               OllamaSettings ??
               EmbeddedSettings ??
               GoogleSettings ??
               HuggingFaceSettings ??
               (AbstractAiSettings)MistralAiSettings;
    }

    internal bool TryGetParametersForGenAiTesting(out string uri, out string apiKey, out string model)
    {
        uri = null;
        apiKey = null;
        model = null;

        var provider = GetActiveProvider();
        switch (provider)
        {
            case AiConnectorType.OpenAi:
                uri = OpenAiSettings.Endpoint;
                apiKey = OpenAiSettings.ApiKey;
                model = OpenAiSettings.Model;
                return true;
            case AiConnectorType.Ollama:
                uri = OllamaSettings.Uri; 
                model = OllamaSettings.Model;
                return true;
        }

        return false;
    }
}
