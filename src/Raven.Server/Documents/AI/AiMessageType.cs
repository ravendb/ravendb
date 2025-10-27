using System;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.AI;

public enum AiResponseType
{
    Result,
    Tool
}

public record AiResponse(AiResponseType Type)
{
    public BlittableJsonReaderObject Result;
    public List<AiToolCall> ToolCalls;
    public BlittableJsonReaderObject Message;
    public DateTime? CreatedUtc { get; init; }
}

public record AiToolCall(string Id, string Name, string Arguments);
