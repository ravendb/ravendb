using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

public class AiAgentArtificialActionResponse : IDynamicJson
{
    public string ToolId;
    public string Content;

    public void Validate()
    {
        if(string.IsNullOrWhiteSpace(ToolId))
            throw new ArgumentException(nameof(ToolId));
        if(string.IsNullOrWhiteSpace(Content))
            throw new ArgumentException(nameof(Content));
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ToolId)] = ToolId,
            [nameof(Content)] = Content
        };
    }
}
