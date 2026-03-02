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

    /// <summary>
    /// Serializes this request to a JSON structure.
    /// </summary>
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(ToolId)] = ToolId,
            [nameof(Arguments)] = Arguments
        };
    }
}
