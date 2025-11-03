using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

public class AiAgentArtificialAction : IDynamicJson
{
    public string ToolName;
    public string Content;

    public void Validate()
    {
        if(string.IsNullOrWhiteSpace(ToolName))
            throw new ArgumentException(nameof(ToolName));
        if(string.IsNullOrWhiteSpace(Content))
            throw new ArgumentException(nameof(Content));
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ToolName)] = ToolName,
            [nameof(Content)] = Content
        };
    }
}
