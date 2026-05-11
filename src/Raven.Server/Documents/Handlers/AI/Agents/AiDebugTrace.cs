using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Raven.Client;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public sealed class AiDebugTrace
{
    internal const string TraceSegment = "request-trace";

    public string RequestBody;

    public BlittableJsonReaderObject Response;
    public List<BlittableJsonReaderObject> StreamEvents;

    public void CaptureRequestBody(Stream captureStream)
    {
        if (captureStream == null)
            return;

        captureStream.Position = 0;
        using var reader = new StreamReader(captureStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
        RequestBody = reader.ReadToEnd();
    }

    public void CaptureResponse(BlittableJsonReaderObject responseContent)
    {
        Response = responseContent.CloneOnTheSameContext();
    }

    public void CaptureSseEvent(JsonOperationContext context, BlittableJsonReaderObject sseEventData)
    {
        StreamEvents ??= [];
        StreamEvents.Add(sseEventData.Clone(context));
    }

    public BlittableJsonReaderObject ToBlittable(JsonOperationContext context, ConversationDocument document, TimeSpan? expiration)
    {
        var metadata = new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Collection] = Constants.Documents.Collections.AiAgentConversationDebugCollection
        };

        if (expiration.HasValue)
            metadata[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.Add(expiration.Value);

        var json = new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Key] = metadata,
            [nameof(ConversationDocument)] = new DynamicJsonValue
            {
                [nameof(ConversationDocument.Id)] = document.Id,
                [nameof(ConversationDocument.Agent)] = document.Agent
            },
            ["MessagesCount"] = document.Messages?.Count ?? 0,
            [nameof(RequestBody)] = RequestBody,
            [nameof(Response)] = Response,
            [nameof(StreamEvents)] = StreamEvents == null ? null : new DynamicJsonArray(StreamEvents)
        };

        return context.ReadObject(json, "ai-agent/debug-trace");
    }
}
