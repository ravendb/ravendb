using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// A user-supplied response for a tool action requested by the model.
/// </summary>
public class AiAgentActionResponse : IDynamicJson
{
    /// <summary>
    /// The tool call identifier.
    /// </summary>
    public string ToolId;

    /// <summary>
    /// Tool response content.
    /// </summary>
    public string Content;

    /// <summary>
    /// Serializes this response to a JSON structure.
    /// </summary>
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ToolId)] = ToolId,
            [nameof(Content)] = Content
        };
    }
}
