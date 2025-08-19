using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// Represents a user-supplied response for a tool action previously requested by the model.
/// </summary>
public class AiAgentActionResponse : IDynamicJson
{
    /// <summary>
    /// The tool identifier corresponding to the original model request.
    /// </summary>
    public string ToolId;
    /// <summary>
    /// The content/value returned for the tool call, serialized as a string (often JSON).
    /// </summary>
    public string Content;
    /// <summary>
    /// Serializes this response to JSON.
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
