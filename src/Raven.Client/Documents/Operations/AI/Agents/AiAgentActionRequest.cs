using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

public class AiAgentActionRequest : IDynamicJson
{
    public string Name;
    public string ToolId;
    public string Arguments;

    [ForceJsonSerialization]
    internal AiAgentActionRequestType Type;

    [ForceJsonSerialization]
    internal string SubConversation;

    public bool IsEqual(AiAgentActionRequest other)
    {
        if (other == null)
            return false;

        return
            ToolId == other.ToolId &&
            Name == other.Name &&
            Arguments == other.Arguments &&
            Type == other.Type &&
            SubConversation == other.SubConversation;
    }

    public override string ToString()
    {
        using (var ctx = JsonOperationContext.ShortTermSingleUse())
            return ctx.ReadObject(ToJson(), string.Empty).ToString();
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(ToolId)] = ToolId,
            [nameof(Arguments)] = Arguments,
            [nameof(Type)] = Type,
            [nameof(SubConversation)] = SubConversation
        };
    }
}

public enum AiAgentActionRequestType
{
    UserAction,
    SubAgent
}
