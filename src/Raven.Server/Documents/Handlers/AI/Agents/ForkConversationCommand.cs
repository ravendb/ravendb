using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Exceptions;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.AI.Agents;

/// <summary>
/// Creates a forked conversation from revision data referenced by a snapshot token.
///
/// The fork process:
/// 1. Load all revisions referenced by the token — fail atomically if any are missing.
/// 2. If the target ID already has a document, delete all its sub-conversations
///    (using SubConversationIds recursively) to clean up orphans.
/// 3. Write the forked documents with adjusted IDs (only the leading prefix is replaced).
/// 4. Adjust SubConversationIds within each document to match the new prefix.
/// </summary>
internal sealed class ForkConversationCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
{
    private readonly DocumentDatabase _database;
    private readonly string _sourceConversationId;
    private readonly string _newConversationId;
    private readonly List<SnapshotRevisionEntry> _revisions;
    private readonly string _expectedChangeVector;

    public string ResultConversationId { get; private set; }
    public string ResultChangeVector { get; private set; }

    public ForkConversationCommand(
        DocumentDatabase database,
        string sourceConversationId,
        string newConversationId,
        List<SnapshotRevisionEntry> revisions,
        string expectedChangeVector = null)
    {
        _database = database;
        _sourceConversationId = sourceConversationId;
        _newConversationId = newConversationId;
        _revisions = revisions;
        _expectedChangeVector = expectedChangeVector;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        // Step 1: Determine the target conversation ID
        var targetId = _newConversationId;
        if (string.IsNullOrEmpty(targetId))
        {
            targetId = Guid.NewGuid().ToString();
        }
        else if (targetId.EndsWith("/"))
        {
            targetId = _database.DocumentsStorage.DocumentPut.BuildDocumentId(targetId, _database.DocumentsStorage.GenerateNextEtag(), out _);
        }
        // Cluster identity ("|") must be resolved before reaching this command by the processor
        Debug.Assert(targetId.EndsWith("|") == false, $"Cluster identity must be resolved before ForkConversationCommand. Got: {targetId}");

        if (_expectedChangeVector != null)
            VerifyExpectedChangeVector(context, targetId);

        // Step 2: Load all revisions — fail atomically if any are missing.
        // Note: we trust the token to an extent — there is no trust boundary between generating
        // and consuming it. The scope validation below is a defense against accidental misuse
        // or corruption, not a security boundary.
        if (_revisions.Any(r => string.Equals(r.Id, _sourceConversationId, StringComparison.OrdinalIgnoreCase)) == false)
        {
            throw new InvalidOperationException(
                $"The snapshot token does not contain a revision for the root conversation '{_sourceConversationId}'.");
        }

        var revisionDocs = new List<(string OriginalId, BlittableJsonReaderObject Data)>(_revisions.Count);
        foreach (var rev in _revisions)
        {
            var revision = _database.DocumentsStorage.RevisionsStorage.GetRevision(context, rev.ChangeVector);
            if (revision == null)
            {
                throw new InvalidOperationException(
                    $"The snapshot token references a revision (document: '{rev.Id}', change vector: '{rev.ChangeVector}') that no longer exists. " +
                    "The revision may have been purged by the revisions retention policy or by an explicit purge operation.");
            }

            // Verify the revision ID is within the source conversation's scope
            if (rev.Id.StartsWith(_sourceConversationId, StringComparison.OrdinalIgnoreCase) == false ||
                (rev.Id.Length > _sourceConversationId.Length && rev.Id[_sourceConversationId.Length] != '/'))
            {
                throw new InvalidOperationException(
                    $"The snapshot token contains a revision for document '{rev.Id}' which is outside " +
                    $"the scope of conversation '{_sourceConversationId}'. Only the root conversation " +
                    "and its sub-conversations (prefixed with the root ID) are allowed.");
            }

            // Verify the revision belongs to the @conversations collection
            if (revision.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) && metadata != null)
            {
                metadata.TryGet(Constants.Documents.Metadata.Collection, out string collection);
                if (collection != null &&
                    string.Equals(collection, Constants.Documents.Collections.AiAgentConversationCollection, StringComparison.OrdinalIgnoreCase) == false)
                {
                    throw new InvalidOperationException(
                        $"The snapshot token references a revision for document '{rev.Id}' which belongs to collection '{collection}', " +
                        $"not the expected '{Constants.Documents.Collections.AiAgentConversationCollection}' collection.");
                }
            }

            revisionDocs.Add((rev.Id, revision.Data));
        }

        // Step 3: If the target already has a document, clean up all sub-conversation documents
        // by reading SubConversationIds from the existing document and recursively deleting them.
        CleanupExistingSubConversations(context, targetId, new HashSet<string>(StringComparer.OrdinalIgnoreCase));

        // Step 4: Write the forked documents with adjusted IDs
        for (int i = 0; i < revisionDocs.Count; i++)
        {
            var (originalId, data) = revisionDocs[i];
            var adjustedId = AdjustId(originalId, _sourceConversationId, targetId);

            // Adjust SubConversationIds within the document to reflect the new prefix
            var adjustedData = AdjustDocumentIds(context, data, _sourceConversationId, targetId);

            // Update @expires metadata if the document has an Expires field
            adjustedData = RefreshExpiration(context, adjustedData, _database.Time.GetUtcNow());

            var putResult = _database.DocumentsStorage.Put(context, adjustedId, null, adjustedData,
                nonPersistentFlags: NonPersistentDocumentFlags.SkipSchemaValidation);

            // Delete existing attachments on the target (relevant for rewind-in-place),
            // then copy attachments from the revision.
            DeleteExistingAttachments(context, adjustedId);
            CopyAttachmentsFromRevision(context, originalId, _revisions[i].ChangeVector, adjustedId);

            if (string.Equals(originalId, _sourceConversationId, StringComparison.OrdinalIgnoreCase))
            {
                // Re-read to get the final change vector (may have changed due to attachment copies)
                var final = _database.DocumentsStorage.Get(context, adjustedId);
                ResultConversationId = final.Id;
                ResultChangeVector = final.ChangeVector;
            }
        }

        return revisionDocs.Count;
    }

    private void VerifyExpectedChangeVector(DocumentsOperationContext context, string targetId)
    {
        using var existingDoc = _database.DocumentsStorage.Get(context, targetId);
        if (_expectedChangeVector == string.Empty)
        {
            if (existingDoc != null)
            {
                throw new ConcurrencyException(
                    $"Expected the conversation '{targetId}' to not exist, but it already exists with change vector '{existingDoc.ChangeVector}'.")
                {
                    ExpectedChangeVector = _expectedChangeVector,
                    ActualChangeVector = existingDoc.ChangeVector,
                    Id = targetId
                };
            }
        }
        else if (existingDoc == null)
        {
            throw new ConcurrencyException(
                $"The conversation '{targetId}' does not exist, but an expected change vector was provided.")
            {
                ExpectedChangeVector = _expectedChangeVector,
                ActualChangeVector = string.Empty,
                Id = targetId
            };
        }
        else if (existingDoc.ChangeVector != _expectedChangeVector)
        {
            throw new ConcurrencyException(
                $"The conversation '{targetId}' was updated and doesn't match the expected change vector. Reload the conversation and try again.")
            {
                ExpectedChangeVector = _expectedChangeVector,
                ActualChangeVector = existingDoc.ChangeVector,
                Id = targetId
            };
        }
    }

    private void DeleteExistingAttachments(DocumentsOperationContext context, string documentId)
    {
        using (DocumentIdWorker.GetLoweredIdSliceFromId(context, documentId, out var lowerId))
        {
            var existing = _database.DocumentsStorage.AttachmentsStorage.GetAttachmentDetailsForDocument(context, lowerId);
            if (existing == null || existing.Count == 0)
                return;

            foreach (var attachment in existing)
            {
                _database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(context, documentId, attachment.Name, null, out _, updateDocument: true);
            }
        }
    }

    /// <summary>
    /// Copies attachments from a revision to the target document using <c>CopyAttachment</c>,
    /// which properly updates the target document's HasAttachments flag.
    /// Reads the revision's <c>@metadata.@attachments</c> to find attachment names,
    /// then copies each one from the revision's attachment storage.
    /// </summary>
    private void CopyAttachmentsFromRevision(DocumentsOperationContext context, string sourceId, string revisionChangeVector, string targetId)
    {
        // Get the revision's attachment list from its metadata
        using var revision = _database.DocumentsStorage.RevisionsStorage.GetRevision(context, revisionChangeVector);
        if (revision == null)
            return;

        if (revision.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false || metadata == null)
            return;

        if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false || attachments == null)
            return;

        using var changeVectorLsv = context.GetLazyString(revisionChangeVector);

        foreach (BlittableJsonReaderObject attachment in attachments)
        {
            if (attachment.TryGet(nameof(AttachmentName.Name), out string name) == false)
                continue;

            _database.DocumentsStorage.AttachmentsStorage.CopyAttachment(context, sourceId, name,
                targetId, name, changeVectorLsv, AttachmentType.Revision);
        }
    }

    /// <summary>
    /// Recursively deletes sub-conversations tracked by the document at <paramref name="documentId"/>.
    /// Reads the SubConversationIds array from the existing document and recurses into each
    /// sub-conversation before deleting it, ensuring nested sub-conversations are also cleaned up.
    /// Documents not tracked in SubConversationIds are left untouched.
    /// </summary>
    private void CleanupExistingSubConversations(DocumentsOperationContext context, string documentId, HashSet<string> visited)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        if (visited.Add(documentId) == false)
            return;

        List<string> subConversationIds = null;
        using (var existingDoc = _database.DocumentsStorage.Get(context, documentId))
        {
            if (existingDoc == null)
                return;

            if (existingDoc.Data.TryGet(nameof(ConversationDocument.SubConversationIds), out BlittableJsonReaderArray subIds) && subIds != null)
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
                CleanupExistingSubConversations(context, subId, visited);
                _database.DocumentsStorage.Delete(context, subId, null);
            }
        }
    }

    /// <summary>
    /// Replaces only the leading conversation ID prefix in a sub-conversation ID.
    /// For example: "chats/42/Search/abc" with source "chats/42" and target "forked/1"
    /// becomes "forked/1/Search/abc". A second occurrence of "chats/42" deeper in the ID
    /// (e.g. "chats/42/chats/42/foo") is NOT replaced — only the leading prefix.
    /// </summary>
    internal static string AdjustId(string originalId, string sourcePrefix, string targetPrefix)
    {
        if (string.Equals(originalId, sourcePrefix, StringComparison.OrdinalIgnoreCase))
            return targetPrefix;

        if (originalId.StartsWith(sourcePrefix, StringComparison.OrdinalIgnoreCase) &&
            originalId.Length > sourcePrefix.Length &&
            originalId[sourcePrefix.Length] == '/')
        {
            return targetPrefix + originalId.Substring(sourcePrefix.Length);
        }

        return originalId;
    }

    private static BlittableJsonReaderObject AdjustDocumentIds(DocumentsOperationContext context, BlittableJsonReaderObject data,
        string sourcePrefix, string targetPrefix)
    {
        if (string.Equals(sourcePrefix, targetPrefix, StringComparison.OrdinalIgnoreCase))
            return data;

        bool modified = false;
        data.Modifications = new DynamicJsonValue(data);

        // Adjust SubConversationIds
        if (data.TryGet(nameof(ConversationDocument.SubConversationIds), out BlittableJsonReaderArray subIds) && subIds != null)
        {
            var adjustedSubIds = new DynamicJsonArray();
            foreach (var item in subIds)
                adjustedSubIds.Add(AdjustId(item.ToString(), sourcePrefix, targetPrefix));

            data.Modifications[nameof(ConversationDocument.SubConversationIds)] = adjustedSubIds;
            modified = true;
        }

        // Adjust OpenActionCalls — entries with SubConversationId need their IDs updated
        if (data.TryGet(nameof(ConversationDocument.OpenActionCalls), out BlittableJsonReaderObject openCalls) && openCalls != null && openCalls.Count > 0)
        {
            var adjustedCalls = new DynamicJsonValue(openCalls);
            bool callsModified = false;

            foreach (var callId in openCalls.GetPropertyNames())
            {
                if (openCalls[callId] is BlittableJsonReaderObject callObj &&
                    callObj.TryGet("SubConversationId", out string subConvId) &&
                    string.IsNullOrEmpty(subConvId) == false)
                {
                    var adjustedSubConvId = AdjustId(subConvId, sourcePrefix, targetPrefix);
                    if (adjustedSubConvId != subConvId)
                    {
                        var adjustedCall = new DynamicJsonValue(callObj);
                        adjustedCall["SubConversationId"] = adjustedSubConvId;
                        adjustedCalls[callId] = adjustedCall;
                        callsModified = true;
                    }
                }
            }

            if (callsModified)
            {
                data.Modifications[nameof(ConversationDocument.OpenActionCalls)] = adjustedCalls;
                modified = true;
            }
        }

        if (modified == false)
            return data;

        return context.ReadObject(data, "adjusted-conversation");
    }

    private static BlittableJsonReaderObject RefreshExpiration(DocumentsOperationContext context, BlittableJsonReaderObject data, DateTime now)
    {
        if (data.TryGet(nameof(ConversationDocument.Expires), out TimeSpan? expires) == false || expires == null)
            return data;

        if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false || metadata == null)
            return data;

        data.Modifications ??= new DynamicJsonValue(data);
        var metadataMod = new DynamicJsonValue(metadata);
        metadataMod[Constants.Documents.Metadata.Expires] = now.Add(expires.Value);
        data.Modifications[Constants.Documents.Metadata.Key] = metadataMod;

        return context.ReadObject(data, "refreshed-expiration");
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
    {
        return new ForkConversationCommandDto
        {
            SourceConversationId = _sourceConversationId,
            NewConversationId = _newConversationId,
            Revisions = _revisions,
            ExpectedChangeVector = _expectedChangeVector
        };
    }

    internal class ForkConversationCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, ForkConversationCommand>
    {
        public string SourceConversationId;
        public string NewConversationId;
        public List<SnapshotRevisionEntry> Revisions;
        public string ExpectedChangeVector;

        public ForkConversationCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new ForkConversationCommand(database, SourceConversationId, NewConversationId, Revisions, ExpectedChangeVector);
        }
    }
}
