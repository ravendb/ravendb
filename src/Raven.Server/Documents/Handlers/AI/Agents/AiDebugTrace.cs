using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public sealed class AiDebugTrace
{
    internal const string TraceSegment = "request-trace";

    public string RequestBody;

    // Names of the attachments sent on this LLM call. Captured separately because
    // the names are not reliably present in the outgoing request JSON on the first
    // call (the "[Attachments: ...]" marker message is only added after the first turn).
    public List<string> AttachmentNames;

    public BlittableJsonReaderObject Response;
    public List<BlittableJsonReaderObject> StreamEvents;

    public void CaptureRequestBody(string request)
    {
        RequestBody = request;
    }

    public void CaptureAttachments(IEnumerable<AiAttachment> attachments)
    {
        if (attachments == null)
            return;

        foreach (var a in attachments)
        {
            AttachmentNames ??= [];
            AttachmentNames.Add(a.Name);
        }
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
            [nameof(AttachmentNames)] = AttachmentNames == null ? null : new DynamicJsonArray(AttachmentNames),
            [nameof(RequestBody)] = RequestBody,
            [nameof(Response)] = Response,
            [nameof(StreamEvents)] = StreamEvents == null ? null : new DynamicJsonArray(StreamEvents)
        };

        return context.ReadObject(json, "ai-agent/debug-trace");
    }
}
