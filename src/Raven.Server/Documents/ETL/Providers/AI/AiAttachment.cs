using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI;

public class AiAttachment
{
    public string Name { get; set; }
    public string Type { get; set; }
    public string DataAsBase64 { get; set; }

    public AiAttachment()
    {
        // for deserialization
    }

    public AiAttachment(string name, string type, string dataAsBase64)
    {
        ValidationMethods.AssertNotNullOrEmpty(name, nameof(Name));
        ValidationMethods.AssertNotNullOrEmpty(type, nameof(Type));
        ValidationMethods.AssertNotNullOrEmpty(dataAsBase64, nameof(DataAsBase64));

        Name = name;
        Type = type;
        DataAsBase64 = dataAsBase64;
    }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(Type)] = Type,
            [nameof(DataAsBase64)] = DataAsBase64
        };

        return json;
    }
}
