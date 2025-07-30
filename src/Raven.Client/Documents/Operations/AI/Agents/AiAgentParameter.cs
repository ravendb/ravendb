using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

public class AiAgentParameter : IDynamicJson
{
    internal AiAgentParameter()
    {
        // for deserialization    
    }

    public AiAgentParameter(string name)
    {
        ValidationMethods.AssertNotNullOrEmpty(name, nameof(name));
        Name = name;
    }

    public AiAgentParameter(string name, string description) : this(name)
    {
        ValidationMethods.AssertNotNullOrEmpty(description, nameof(description));
        Description = description;
    }

    public string Name { get; set; }
    public string Description { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(Description)] = Description
        };
    }
}
