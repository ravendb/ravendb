using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// Represents a model-initiated request to execute a tool action, including the function name,
/// tool identifier, and serialized arguments.
/// </summary>
public class AiAgentActionRequest : IDynamicJson
{
    /// <summary>
    /// The function name of the tool to be invoked.
    /// </summary>
    public string Name;
    /// <summary>
    /// The tool identifier assigned by the server when the tool was defined.
    /// </summary>
    public string ToolId;
    /// <summary>
    /// A JSON string representing the arguments for the tool call.
    /// </summary>
    public string Arguments;
    /// <summary>
    /// Serializes this request to JSON.
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
