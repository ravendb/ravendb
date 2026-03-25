using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

public class AiAgentToolSubAgent : IDynamicJson
{
    /// <summary>
    /// The identifier of the sub-agent that we can call
    /// </summary>
    public string Identifier;

    /// <summary>
    /// The description for the sub-agent (which the model will use
    /// to decided when to call it)
    /// </summary>
    public string Description;

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Identifier)] = Identifier,
            [nameof(Description)] = Description
        };
    }
}
