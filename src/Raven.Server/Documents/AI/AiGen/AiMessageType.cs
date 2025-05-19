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
