using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// An artificial tool call response added to the conversation context.
/// </summary>
public class AiAgentArtificialActionResponse : IDynamicJson
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
    /// Validates the current instance.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when <see cref="ToolId"/> or <see cref="Content"/> is <see langword="null"/> or whitespace.</exception>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ToolId))
            throw new ArgumentException(nameof(ToolId));
        if (string.IsNullOrWhiteSpace(Content))
            throw new ArgumentException(nameof(Content));
    }

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
