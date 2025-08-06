using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

public class GenAiAttachment
{
    public string Name { get; set; }
    public string Type { get; set; }
    public string Data { get; set; }

    public GenAiAttachment()
    {
        // for deserialization
    }
    public GenAiAttachment(string name, string type, string data)
    {
        Name = name;
        Type = type;
        Data = data;
    }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(Type)] = Type,
            [nameof(Data)] = Data
        };

        return json;
    }
}

public record GenAiScriptResult(string DocumentId, BlittableJsonReaderObject Context, string AiHash, bool IsCached)
{
    public List<GenAiAttachment> Attachments;
}
