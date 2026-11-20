using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.AI;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

/// <summary>
/// Processor for GET /ai/agent/snapshots — lists available snapshots for a conversation.
/// </summary>
internal sealed class AiAgentProcessorForGetConversationSnapshots : AbstractDatabaseHandlerProcessor<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AiAgentProcessorForGetConversationSnapshots([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var conversationId = RequestHandler.GetStringQueryString("conversationId");
        var pageSize = RequestHandler.GetPageSize(25);
        var before = RequestHandler.GetDateTimeQueryString("before", required: false);

        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var revisionsStorage = RequestHandler.Database.DocumentsStorage.RevisionsStorage;

            var snapshots = new List<(string Token, DateTime CreatedAt)>();

            // We return all revisions, not just force-created snapshots. Users may also
            // have collection-level revisions configured, and those are equally valid fork points.
            foreach (var revision in revisionsStorage.GetRevisionsByDate(context, conversationId, take: pageSize, before: before))
            {
                using (revision)
                {
                    var revisionEntries = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        [conversationId] = revision.ChangeVector
                    };

                    // Recursively collect sub-conversation revisions (handles nested sub-agents).
                    // Missing sub-conversation revisions are skipped, producing a partial token.
                    CollectSubConversationRevisions(context, revisionsStorage, revision, revisionEntries);

                    var token = SnapshotTokenDto.Build(context, conversationId, revision.LastModified, revisionEntries);
                    snapshots.Add((token, revision.LastModified));
                }
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Snapshots");
                writer.WriteStartArray();

                bool first = true;
                foreach (var (token, createdAt) in snapshots)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(AiConversationSnapshot.Token));
                    writer.WriteString(token);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(AiConversationSnapshot.CreatedAt));
                    writer.WriteDateTime(createdAt, isUtc: true);
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
            }
        }
    }

    /// <summary>
    /// Recursively collects sub-conversation revisions from a revision's SubConversationIds.
    /// Sub-conversations whose revision is not found are skipped — the resulting token will
    /// be partial but still usable for forking the sub-conversations that are available.
    /// </summary>
    private static void CollectSubConversationRevisions(
        DocumentsOperationContext context,
        Raven.Server.Documents.Revisions.RevisionsStorage revisionsStorage,
        Document revision,
        Dictionary<string, string> revisionEntries)
    {
        System.Runtime.CompilerServices.RuntimeHelpers.EnsureSufficientExecutionStack();

        if (revision.Data.TryGet(nameof(ConversationDocument.SubConversationIds), out BlittableJsonReaderArray subIds) == false || subIds == null)
            return;

        foreach (var subIdValue in subIds)
        {
            var subId = subIdValue.ToString();
            if (revisionEntries.ContainsKey(subId))
                continue;

            using var subRevision = revisionsStorage.GetRevisionBefore(context, subId, revision.LastModified.AddTicks(1));
            if (subRevision == null)
                continue; // skip this sub-conversation, collect the rest

            revisionEntries[subId] = subRevision.ChangeVector;

            CollectSubConversationRevisions(context, revisionsStorage, subRevision, revisionEntries);
        }
    }
}
