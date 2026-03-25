using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// A model-initiated request to execute a tool action.
/// </summary>
public class AiAgentActionRequest : IDynamicJson
{
    /// <summary>
    /// The tool name.
    /// </summary>
    public string Name;

    /// <summary>
    /// The tool call identifier.
    /// </summary>
    public string ToolId;

    /// <summary>
    /// Tool arguments as a JSON string.
    /// </summary>
    public string Arguments;

    [ForceJsonSerialization]
    internal AiAgentActionRequestType Type;

    [ForceJsonSerialization]
    internal string SubConversationId;

    internal bool IsEqual(AiAgentActionRequest other)
    {
        if (other == null)
            return false;

        return
            ToolId == other.ToolId &&
            Name == other.Name &&
            Arguments == other.Arguments &&
            Type == other.Type &&
            SubConversationId == other.SubConversationId;
    }

    public override string ToString()
    {
        using (var ctx = JsonOperationContext.ShortTermSingleUse())
            return ctx.ReadObject(ToJson(), string.Empty).ToString();
    }


    /// <summary>
    /// Serializes this request to a JSON structure.
    /// </summary>
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(ToolId)] = ToolId,
            [nameof(Arguments)] = Arguments,
            [nameof(Type)] = Type,
            [nameof(SubConversationId)] = SubConversationId
        };
    }
}

public enum AiAgentActionRequestType
{
    UserAction,
    SubAgent
}
