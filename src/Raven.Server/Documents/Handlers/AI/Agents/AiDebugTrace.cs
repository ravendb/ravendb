using System;
using System.Collections.Generic;
using Raven.Client;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public sealed class AiDebugTrace
{
    internal const string TraceSegment = "request-trace";

    // Fallback TTL applied when the parent conversation has no expiration, so trace
    // documents never accumulate indefinitely on servers without conversation expiry.
    internal static readonly TimeSpan DefaultMaxExpiration = TimeSpan.FromDays(7);

    public BlittableJsonReaderObject Request;
    public BlittableJsonReaderObject Response;
    public List<BlittableJsonReaderObject> StreamEvents;

    public BlittableJsonReaderObject ToBlittable(JsonOperationContext context, ConversationDocument document, TimeSpan? expiration)
    {
        var effectiveExpiration = expiration ?? DefaultMaxExpiration;

        var metadata = new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Collection] = Constants.Documents.Collections.AiAgentConversationDebugCollection,
            [Constants.Documents.Metadata.Expires] = DateTime.UtcNow.Add(effectiveExpiration)
        };

        var json = new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Key] = metadata,
            [nameof(ConversationDocument)] = new DynamicJsonValue
            {
                [nameof(ConversationDocument.Id)] = document.Id,
                [nameof(ConversationDocument.Agent)] = document.Agent
            },
            ["MessagesCount"] = document.Messages?.Count ?? 0,
            [nameof(Request)] = Request,
            [nameof(Response)] = Response,
            [nameof(StreamEvents)] = StreamEvents == null ? null : new DynamicJsonArray(StreamEvents)
        };

        return context.ReadObject(json, "ai-agent/debug-trace");
    }
}
