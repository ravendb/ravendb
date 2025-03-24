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

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(Identifier)] = Identifier;
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
}
