using System.Collections.Generic;
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
        get => _identifier ?? GenerateIdentifierFromName(Name);
        set => _identifier = NormalizeIdentifier(value);
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
        var settings = new List<string>();

        if (OpenAiSettings != null)
        {
            settings.Add(nameof(OpenAiSettings));
            if (OpenAiSettings.HasSettings() == false)
                errors.Add($"{nameof(OpenAiSettings)} has no valid setting. '{nameof(OpenAiSettings.ApiKey)}' has null or empty value");
        }

        if (AzureOpenAiSettings != null)
        {
            settings.Add(nameof(AzureOpenAiSettings));
            if (AzureOpenAiSettings.HasSettings() == false)
                errors.Add($"{nameof(AzureOpenAiSettings)} has no valid setting. '{nameof(AzureOpenAiSettings.ApiKey)}' has null or empty value");
        }

        if (OllamaSettings != null)
        {
            settings.Add(nameof(OllamaSettings));
            if (OllamaSettings.HasSettings() == false)
                errors.Add($"{nameof(OllamaSettings)} has no valid setting. '{nameof(OllamaSettings.Uri)}' and '{nameof(OllamaSettings.Model)}' are both have null or empty values");
        }

        if (OnnxSettings != null)
        {
            settings.Add(nameof(OnnxSettings));
            if (OnnxSettings.HasSettings() == false)
                errors.Add($"{nameof(OnnxSettings)} has no valid setting.");
        }

        if (GoogleSettings != null)
        {
            settings.Add(nameof(GoogleSettings));
            if (GoogleSettings.HasSettings() == false)
                errors.Add($"{nameof(GoogleSettings)} has no valid setting. '{nameof(GoogleSettings.ApiKey)}' has null or empty value");
        }

        var identifier = Identifier;
        if (string.IsNullOrEmpty(identifier))
            errors.Add("Could not generate valid identifier from name. Please specify identifier explicitly.");

        switch (settings.Count)
        {
            case 0:
                errors.Add($"At least one of the following settings must be set: {string.Join(", ", nameof(OllamaSettings), nameof(OpenAiSettings), nameof(OnnxSettings))}");
                break;
            case > 1:
                errors.Add($"Only one of the following settings can be set: {string.Join(", ", settings)}");
                break;
        }
    }

    private static string GenerateIdentifierFromName(string name)
    {
        if (string.IsNullOrEmpty(name))
            return null;

        return NormalizeIdentifier(name);
    }

    private static string NormalizeIdentifier(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return null;

        var result = new StringBuilder();
        var lastWasHyphen = false;

        // First normalize to FormD to separate letters from their diacritics
        foreach (var c in input.Normalize(NormalizationForm.FormD))
        {
            // Check if this is a letter that needs to be preserved
            if (c >= 'a' && c <= 'z' || c >= '0' && c <= '9')
            {
                result.Append(c);
                lastWasHyphen = false;
            }
            else if (c >= 'A' && c <= 'Z')
            {
                result.Append(char.ToLowerInvariant(c));
                lastWasHyphen = false;
            }
            else if (!lastWasHyphen && result.Length > 0) // Add hyphen for any other character
            {
                result.Append('-');
                lastWasHyphen = true;
            }
        }

        // Trim any trailing hyphens
        var finalResult = result.ToString().TrimEnd('-');

        // Ensure we have at least one character
        return string.IsNullOrEmpty(finalResult) ? "default" : finalResult;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(Identifier)] = Identifier;
        json[nameof(OllamaSettings)] = OllamaSettings?.ToJson();
        json[nameof(OpenAiSettings)] = OpenAiSettings?.ToJson();
        json[nameof(OnnxSettings)] = OnnxSettings?.ToJson();
        json[nameof(GoogleSettings)] = GoogleSettings?.ToJson();
        json[nameof(AzureOpenAiSettings)] = AzureOpenAiSettings?.ToJson();

        return json;
    }
}
