using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class AiConnectionString : ConnectionString
{
    private string _identifier;

    /// <summary>
    /// A unique identifier used in document paths.
    /// Only English letters, numbers and hyphens are allowed.
    /// If not specified, will be auto-generated from the connection name.
    /// </summary>
    public string Identifier
    {
        get => _identifier ?? GenerateIdentifier(Name);
        set => _identifier = GenerateIdentifier(value);
    }

    public OpenAiSettings OpenAiSettings { get; set; }

    public AzureOpenAiSettings AzureOpenAiSettings { get; set; }

    public OllamaSettings OllamaSettings { get; set; }

    public OnnxSettings OnnxSettings { get; set; }

    public GoogleSettings GoogleSettings { get; set; }

    public HuggingFaceSettings HuggingFaceSettings { get; set; }

    public override ConnectionStringType Type => ConnectionStringType.Ai;

    protected override void ValidateImpl(ref List<string> errors)
    {
        var settings = new AbstractAiSettings[]
        {
            OpenAiSettings,
            AzureOpenAiSettings,
            OllamaSettings,
            OnnxSettings,
            GoogleSettings,
            HuggingFaceSettings
        }.Where(s => s != null).ToList();

        foreach (var setting in settings)
        {
            if (setting.HasSettings() == false)
                errors.Add($"{setting.GetType().Name} has invalid configuration");
        }

        if (string.IsNullOrEmpty(Identifier))
            errors.Add("Could not generate valid identifier from name. Please specify identifier explicitly.");

        switch (settings.Count)
        {
            case 0:
                errors.Add($"At least one of the following settings must be set: {string.Join(", ", nameof(OllamaSettings), nameof(OpenAiSettings), nameof(OnnxSettings))}");
                break;
            case > 1:
                var configuredNames = settings.Select(s => s.GetType().Name);
                errors.Add($"Only one of the following settings can be set: {string.Join(", ", configuredNames)}");
                break;
        }
    }

    private static string GenerateIdentifier(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return null;

        var result = new StringBuilder();
        var lastWasHyphen = false;

        // First normalize to FormD to separate letters from their diacritics
        foreach (var c in input.Normalize(NormalizationForm.FormD))
        {
            // Check if this is a letter that needs to be preserved
            if (c is
                >= 'a' and <= 'z' or
                >= '0' and <= '9')
            {
                result.Append(c);
                lastWasHyphen = false;
            }
            else if (c is >= 'A' and <= 'Z')
            {
                result.Append(char.ToLowerInvariant(c));
                lastWasHyphen = false;
            }
            else if (lastWasHyphen == false && result.Length > 0) // Add hyphen for any other character
            {
                result.Append('-');
                lastWasHyphen = true;
            }
        }

        // Trim any trailing hyphens
        var finalResult = result.ToString().TrimEnd('-');

        // Ensure we have at least one character
        return string.IsNullOrEmpty(finalResult) ? $"{nameof(AiConnectionString)}Identifier" : finalResult;
    }

    public bool HasCriticalChanges(AiConnectionString newConnectionString)
    {
        if (newConnectionString == null)
            return true;

        if (Identifier != newConnectionString.Identifier)
            return true;

        var oldProvider = GetActiveProvider();
        var newProvider = newConnectionString.GetActiveProvider();

        if (oldProvider != newProvider)
            return true;

        return oldProvider switch
        {
            AiConnectorType.OpenAi => OpenAiSettings.HasCriticalChanges(newConnectionString.OpenAiSettings),
            AiConnectorType.AzureOpenAI => AzureOpenAiSettings.HasCriticalChanges(newConnectionString.AzureOpenAiSettings),
            AiConnectorType.Ollama => OllamaSettings.HasCriticalChanges(newConnectionString.OllamaSettings),
            AiConnectorType.Onnx => OnnxSettings.HasCriticalChanges(newConnectionString.OnnxSettings),
            AiConnectorType.Google => GoogleSettings.HasCriticalChanges(newConnectionString.GoogleSettings),
            AiConnectorType.HuggingFace => HuggingFaceSettings.HasCriticalChanges(newConnectionString.HuggingFaceSettings),
            _ => true
        };
    }

    private AiConnectorType GetActiveProvider()
    {
        if (OpenAiSettings != null)
            return AiConnectorType.OpenAi;
        if (AzureOpenAiSettings != null)
            return AiConnectorType.AzureOpenAI;
        if (OllamaSettings != null)
            return AiConnectorType.Ollama;
        if (OnnxSettings != null)
            return AiConnectorType.Onnx;
        if (GoogleSettings != null)
            return AiConnectorType.Google;
        if (HuggingFaceSettings != null)
            return AiConnectorType.HuggingFace;

        return AiConnectorType.None;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(Identifier)] = Identifier;
        json[nameof(OpenAiSettings)] = OpenAiSettings?.ToJson();
        json[nameof(AzureOpenAiSettings)] = AzureOpenAiSettings?.ToJson();
        json[nameof(OllamaSettings)] = OllamaSettings?.ToJson();
        json[nameof(OnnxSettings)] = OnnxSettings?.ToJson();
        json[nameof(GoogleSettings)] = GoogleSettings?.ToJson();
        json[nameof(HuggingFaceSettings)] = HuggingFaceSettings?.ToJson();

        return json;
    }
}
