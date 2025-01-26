using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class AiConnectionString : ConnectionString
{
    public OpenAiSettings OpenAiSettings { get; set; }

    public OllamaSettings OllamaSettings { get; set; }

    public OnnxSettings OnnxSettings { get; set; }

    public override ConnectionStringType Type => ConnectionStringType.Ai;

    protected override void ValidateImpl(ref List<string> errors)
    {
        if (OllamaSettings != null)
        {
            if (OllamaSettings.HasSettings() == false)
                errors.Add($"{nameof(OllamaSettings)} has no valid setting. '{nameof(OllamaSettings.Uri)}' and '{nameof(OllamaSettings.Model)}' are both have null or empty values");
        }

        if (OpenAiSettings != null)
        {
            if (OpenAiSettings.HasSettings() == false)
                errors.Add($"{nameof(OpenAiSettings)} has no valid setting. '{nameof(OpenAiSettings.ApiKey)}' has null or empty value");
        }

        if (OnnxSettings != null)
        {
            if (OnnxSettings.HasSettings() == false)
                errors.Add($"{nameof(OnnxSettings)} has no valid setting.");
        }
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(OllamaSettings)] = OllamaSettings?.ToJson();
        json[nameof(OpenAiSettings)] = OpenAiSettings?.ToJson();
        json[nameof(OnnxSettings)] = OnnxSettings?.ToJson();

        return json;
    }
}
