using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI.AiGen;

public enum AiMessageType
{
    System,
    User,
    Tool,
    ToolReply,
    Assistant
}

public enum AiResponseType
{
    Result,
    Tool
}

public record AiResponse(AiResponseType Type)
{
    public BlittableJsonReaderObject Result;
    public List<AiToolCall> ToolCalls;
}

public record AiToolCall(string Id, string Name, string Arguments);

public record AiToolResponse(string Id, string Content);

public record AiMessage(AiMessageType Type) : IDynamicJson
{
    public string Message;
    public List<AiToolCall> ToolCalls;
    public string ToolCallId;
    
    private string GetRole()
    {
        return Type switch
        {
            AiMessageType.System => "system",
            AiMessageType.User => "user",
            AiMessageType.Tool =>"assistant",
            AiMessageType.ToolReply  => "tool",
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public DynamicJsonValue ToJson()
    {
        var djv = new DynamicJsonValue
        {
            ["role"] = GetRole(),
            ["content"] = Message,
        };
        if (ToolCalls is not null)
        {
            var calls = new DynamicJsonArray();
            djv["tool_calls"] = calls;
            foreach (var call in ToolCalls)
            {
                calls.Add(new DynamicJsonValue
                {
                    ["id"]  = call.Id,
                    ["type"] = "function",
                    ["function"] = new DynamicJsonValue
                    {
                        ["name"] = call.Name,
                        ["arguments"] = call.Arguments,
                    }
                });
            }
        }

        if (ToolCallId is not null)
        {
            djv["tool_call_id"] = ToolCallId;
        }
        return djv;
    }
}
