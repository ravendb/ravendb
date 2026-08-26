using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Handlers.AI.Agents;

/// <summary>
/// Transaction merger command that creates revisions (snapshots) for a conversation
/// and all its sub-conversations recursively, then builds a JSON snapshot token.
/// </summary>
internal sealed class CreateConversationSnapshotCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
{
    private readonly DocumentDatabase _database;
    private readonly string _conversationId;

    public string SnapshotToken { get; private set; }
    public DateTime CreatedAt { get; private set; }

    public CreateConversationSnapshotCommand(DocumentDatabase database, string conversationId)
    {
        _database = database;
        _conversationId = conversationId;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        var revisions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        CollectRevisionsRecursive(context, _conversationId, revisions);

        if (revisions.Count == 0)
            return 0;

        if (revisions.TryGetValue(_conversationId, out var rootCv) == false)
            throw new InvalidOperationException($"Root conversation '{_conversationId}' was not found in collected revisions.");

        using var rootRevision = _database.DocumentsStorage.RevisionsStorage.GetRevision(context, rootCv);
        CreatedAt = rootRevision?.LastModified ?? _database.Time.GetUtcNow();

        SnapshotToken = SnapshotTokenDto.Build(context, _conversationId, CreatedAt, revisions);
        return 1;
    }

    /// <summary>
    /// Recursively force-creates a revision for the given document and all its sub-conversations.
    /// </summary>
    private void CollectRevisionsRecursive(DocumentsOperationContext context, string documentId, Dictionary<string, string> revisions)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        if (revisions.ContainsKey(documentId))
            return;

        var cv = _database.DocumentsStorage.RevisionsStorage.ForceCreateRevision(context, documentId);
        if (cv == null)
            return;

        revisions[documentId] = cv;

        List<string> subConversationIds = null;
        using (var document = _database.DocumentsStorage.Get(context, documentId))
        {
            if (document?.Data.TryGet(nameof(ConversationDocument.SubConversationIds), out BlittableJsonReaderArray subIds) == true && subIds != null)
            {
                subConversationIds = new List<string>(subIds.Length);
                foreach (var subId in subIds)
                {
                    subConversationIds.Add(subId.ToString());
                }
            }
        }

        if (subConversationIds != null)
        {
            foreach (var subId in subConversationIds)
            {
                CollectRevisionsRecursive(context, subId, revisions);
            }
        }
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
    {
        return new CreateConversationSnapshotCommandDto { ConversationId = _conversationId };
    }

    internal class CreateConversationSnapshotCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, CreateConversationSnapshotCommand>
    {
        public string ConversationId;

        public CreateConversationSnapshotCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new CreateConversationSnapshotCommand(database, ConversationId);
        }
    }
}

internal sealed class SnapshotTokenDto : IDynamicJson
{
    public string ConversationId { get; set; }
    public DateTime CreatedAt { get; set; }
    public List<SnapshotRevisionEntry> Revisions { get; set; }

    public DynamicJsonValue ToJson()
    {
        var revisionArray = new DynamicJsonArray();
        foreach (var rev in Revisions)
            revisionArray.Add(rev.ToJson());

        return new DynamicJsonValue
        {
            [nameof(ConversationId)] = ConversationId,
            [nameof(CreatedAt)] = CreatedAt,
            [nameof(Revisions)] = revisionArray
        };
    }

    public static string Build(JsonOperationContext context, string conversationId, DateTime createdAt, Dictionary<string, string> revisions)
    {
        var dto = new SnapshotTokenDto
        {
            ConversationId = conversationId,
            CreatedAt = createdAt,
            Revisions = new List<SnapshotRevisionEntry>(revisions.Count)
        };

        // Root conversation first, then sub-conversations
        if (revisions.Remove(conversationId, out var rootCv))
            dto.Revisions.Add(new SnapshotRevisionEntry { Id = conversationId, ChangeVector = rootCv });

        foreach (var (id, cv) in revisions)
            dto.Revisions.Add(new SnapshotRevisionEntry { Id = id, ChangeVector = cv });

        using var blittable = context.ReadObject(dto.ToJson(), "snapshot-token");
        return blittable.ToString();
    }

    public static SnapshotTokenDto Parse(JsonOperationContext context, string token)
    {
        if (string.IsNullOrEmpty(token))
            throw new InvalidOperationException("Snapshot token is empty.");

        string Truncated() => token.Length > 200 ? token[..200] + "..." : token;

        SnapshotTokenDto dto;
        try
        {
            using var obj = context.Sync.ReadForMemory(token, "snapshot-token");
            dto = Raven.Server.Json.JsonDeserializationServer.SnapshotTokenDto(obj);
        }
        catch (Exception e)
        {
            throw new InvalidOperationException($"Invalid snapshot token format. Expected a valid JSON string, but got: '{Truncated()}'", e);
        }

        if (string.IsNullOrEmpty(dto.ConversationId))
            throw new InvalidOperationException($"Invalid snapshot token: missing ConversationId. Token: '{Truncated()}'");

        if (dto.Revisions == null || dto.Revisions.Count == 0)
            throw new InvalidOperationException($"Invalid snapshot token: Revisions array is missing or empty. At least one revision (the main conversation) is required. Token: '{Truncated()}'");

        foreach (var rev in dto.Revisions)
        {
            if (string.IsNullOrEmpty(rev.Id) || string.IsNullOrEmpty(rev.ChangeVector))
                throw new InvalidOperationException($"Invalid snapshot token: each revision must have an Id and ChangeVector. Token: '{Truncated()}'");
        }

        return dto;
    }
}

internal sealed class SnapshotRevisionEntry : IDynamicJson
{
    public string Id { get; set; }
    public string ChangeVector { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(ChangeVector)] = ChangeVector
        };
    }
}
