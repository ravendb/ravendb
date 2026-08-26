using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Handlers.AI.Agents;

/// <summary>
/// Deletes revisions for a conversation and its sub-conversations (found via SubConversationIds recursively).
/// When <c>Before</c> is specified, only revisions older than that date are deleted
/// using per-revision <c>DeleteRevision</c> calls. When null, all revisions are deleted.
/// All conversation documents belong to the <c>@conversations</c> collection.
/// </summary>
internal sealed class PurgeConversationSnapshotsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
{
    private readonly DocumentDatabase _database;
    private readonly string _conversationId;
    private readonly DateTime? _before;

    public PurgeConversationSnapshotsCommand(DocumentDatabase database, string conversationId, DateTime? before = null)
    {
        _database = database;
        _conversationId = conversationId;
        _before = before;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        // Validate the root document belongs to the @conversations collection
        using (var rootDoc = _database.DocumentsStorage.Get(context, _conversationId))
        {
            if (rootDoc == null)
                return 0;

            if (rootDoc.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) && metadata != null)
            {
                metadata.TryGet(Constants.Documents.Metadata.Collection, out string collection);
                if (collection != null &&
                    string.Equals(collection, Constants.Documents.Collections.AiAgentConversationCollection, StringComparison.OrdinalIgnoreCase) == false)
                {
                    throw new InvalidOperationException(
                        $"Cannot purge snapshots for '{_conversationId}': document belongs to collection '{collection}', " +
                        $"not '{Constants.Documents.Collections.AiAgentConversationCollection}'.");
                }
            }
        }

        // Collect all document IDs to purge: the main conversation + all sub-conversations.
        var documentIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        CollectSubConversationIdsRecursive(context, _conversationId, documentIds);

        foreach (var documentId in documentIds)
            PurgeRevisionsFor(context, documentId);

        return documentIds.Count;
    }

    /// <summary>
    /// Recursively collects the document ID and all SubConversationIds from the current document.
    /// </summary>
    private void CollectSubConversationIdsRecursive(DocumentsOperationContext context, string documentId, HashSet<string> collected)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        if (collected.Add(documentId) == false)
            return;

        List<string> subConversationIds = null;
        using (var doc = _database.DocumentsStorage.Get(context, documentId))
        {
            if (doc?.Data.TryGet(nameof(ConversationDocument.SubConversationIds), out BlittableJsonReaderArray subIds) == true && subIds != null)
            {
                subConversationIds = new List<string>(subIds.Length);
                foreach (var subIdValue in subIds)
                {
                    subConversationIds.Add(subIdValue.ToString());
                }
            }
        }

        if (subConversationIds != null)
        {
            foreach (var subId in subConversationIds)
            {
                CollectSubConversationIdsRecursive(context, subId, collected);
            }
        }
    }

    private void PurgeRevisionsFor(DocumentsOperationContext context, string documentId)
    {
        if (_before.HasValue == false)
        {
            _database.DocumentsStorage.RevisionsStorage.ForceDeleteAllRevisionsFor(context, documentId);
            return;
        }

        // Collect change vectors of revisions older than the cutoff, then delete individually.
        var changeVectorsToDelete = new List<string>();

        foreach (var revision in _database.DocumentsStorage.RevisionsStorage.GetRevisionsByDate(context, documentId, before: _before))
        {
            changeVectorsToDelete.Add(revision.ChangeVector);
        }

        if (changeVectorsToDelete.Count == 0)
            return;

        var lastModifiedTicks = _database.Time.GetUtcNow().Ticks;
        string collection = Constants.Documents.Collections.AiAgentConversationCollection;

        foreach (string changeVector in changeVectorsToDelete)
        {
            _database.DocumentsStorage.RevisionsStorage.DeleteRevision(context, documentId, collection, changeVector, lastModifiedTicks);
        }
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
    {
        return new PurgeConversationSnapshotsCommandDto
        {
            ConversationId = _conversationId,
            Before = _before
        };
    }

    internal class PurgeConversationSnapshotsCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, PurgeConversationSnapshotsCommand>
    {
        public string ConversationId;
        public DateTime? Before;

        public PurgeConversationSnapshotsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new PurgeConversationSnapshotsCommand(database, ConversationId, Before);
        }
    }
}
