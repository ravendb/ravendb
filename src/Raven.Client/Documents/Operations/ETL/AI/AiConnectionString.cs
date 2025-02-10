using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class AiConnectionString : ConnectionString
{
    public string Identifier { get; set; }

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
            setting.ValidateMandatoryFields(ref errors);

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

    internal string GenerateIdentifier() => GenerateIdentifier(Name);

    private string GenerateIdentifier(string input)
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

    internal bool ValidateIdentifier(out List<string> errors)
    {
        errors = [];

        if (string.IsNullOrWhiteSpace(Identifier))
        {
            errors.Add("Identifier cannot be empty or contain only whitespace;");
            return false;
        }

        // Check that the string is already normalized (contains only a-z, 0-9 and hyphens)
        if (Identifier != Identifier.Normalize(NormalizationForm.FormD))
            errors.Add("Identifier contains diacritical marks or non-ASCII characters;");

        // Check that there are no uppercase letters
        if (Identifier.Any(char.IsUpper))
            errors.Add("Identifier contains uppercase letters;");

        // Check for invalid characters and collect them
        var invalidChars = Identifier.Where(c => c is not (>= 'a' and <= 'z' or >= '0' and <= '9' or '-'))
            .Distinct()
            .ToList();
        if (invalidChars.Count != 0)
            errors.Add($"Identifier contains invalid characters: {string.Join(", ", invalidChars.Select(c => $"'{c}'"))}. " +
                       $"Only lowercase letters (a-z), numbers (0-9) and hyphens (-) are allowed.");

        // Check that there are no consecutive hyphens
        if (Identifier.Contains("--"))
            errors.Add("Identifier contains consecutive hyphens;");

        // Check that the string does not end with a hyphen
        if (Identifier.EndsWith("-"))
            errors.Add("Identifier ends with a hyphen;");

        return errors.Count != 0 == false;
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
            AiConnectorType.Onnx => OnnxSettings.Compare(newConnectionString.OnnxSettings),
            AiConnectorType.Google => GoogleSettings.Compare(newConnectionString.GoogleSettings),
            AiConnectorType.HuggingFace => HuggingFaceSettings.Compare(newConnectionString.HuggingFaceSettings),
            _ => AiSettingsCompareDifferences.All
        };

        return result;
    }

    private AiConnectorType GetActiveProvider()
    {
        if (OpenAiSettings != null)
            return AiConnectorType.OpenAi;
        if (AzureOpenAiSettings != null)
            return AiConnectorType.AzureOpenAi;
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
