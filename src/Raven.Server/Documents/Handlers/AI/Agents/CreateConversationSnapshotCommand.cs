using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
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
    public string RootChangeVector { get; private set; }
    public string RootChangeVectorBeforeSnapshot { get; private set; }

    public CreateConversationSnapshotCommand(DocumentDatabase database, string conversationId)
    {
        _database = database;
        _conversationId = conversationId;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        // document ID -> (change vector, token key) of its snapshot revision; the root goes first in the token
        var revisions = new Dictionary<string, (string ChangeVector, string TokenKey)>(StringComparer.OrdinalIgnoreCase);

        using (var root = _database.DocumentsStorage.Get(context, _conversationId, DocumentFields.ChangeVector))
            RootChangeVectorBeforeSnapshot = root?.ChangeVector;

        CollectRevisionsRecursive(context, _conversationId, revisions);

        if (revisions.Count == 0)
            return 0;

        if (revisions.Remove(_conversationId, out var rootRevisionEntry) == false)
            throw new InvalidOperationException($"Root conversation '{_conversationId}' was not found in collected revisions.");

        using var rootRevision = _database.DocumentsStorage.RevisionsStorage.GetRevision(context, rootRevisionEntry.ChangeVector);
        CreatedAt = rootRevision?.LastModified ?? _database.Time.GetUtcNow();
        RootChangeVector = rootRevisionEntry.ChangeVector;

        var keys = new List<string>(revisions.Count + 1) { rootRevisionEntry.TokenKey };
        foreach (var entry in revisions.Values)
            keys.Add(entry.TokenKey);

        SnapshotToken = Agents.SnapshotToken.Build(_conversationId, keys);
        return 1;
    }

    /// <summary>
    /// Recursively force-creates a revision for the given document and all its sub-conversations.
    /// </summary>
    private void CollectRevisionsRecursive(DocumentsOperationContext context, string documentId, Dictionary<string, (string ChangeVector, string TokenKey)> revisions)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        if (revisions.ContainsKey(documentId))
            return;

        var revisionsStorage = _database.DocumentsStorage.RevisionsStorage;
        var cv = revisionsStorage.ForceCreateRevision(context, documentId);
        if (cv == null)
            return;

        // a revision written before revisions were keyed by hash cannot be in a token
        var tokenKey = revisionsStorage.GetRevisionTokenKey(context, cv) ??
                       throw new NotSupportedException($"Cannot snapshot '{documentId}': its revision with change vector '{cv}' was written in the old revision format.");

        revisions[documentId] = (cv, tokenKey);

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

/// <summary>
/// A snapshot token: "{conversation ID}.{revision keys}". Each revision key is the 24-byte primary key of a revision in
/// base64url, 32 characters, the root conversation first. Base64url has no '.', so the token splits at its last '.'.
/// </summary>
internal readonly struct SnapshotToken
{
    public const int RevisionKeyLength = 32;
    private const char Separator = '.';

    private readonly string _token;
    private readonly int _separator;

    private SnapshotToken(string token, int separator)
    {
        _token = token;
        _separator = separator;
    }

    public ReadOnlySpan<char> ConversationId => _token.AsSpan(0, _separator);

    public int RevisionCount => (_token.Length - _separator - 1) / RevisionKeyLength;

    /// <summary>The revision key at <paramref name="index"/>; index 0 is the root conversation's revision</summary>
    public ReadOnlySpan<char> GetRevisionKey(int index) => _token.AsSpan(_separator + 1 + index * RevisionKeyLength, RevisionKeyLength);

    public override string ToString() => _token;

    public static string Build(string conversationId, List<string> revisionKeys)
    {
        var sb = new StringBuilder(conversationId.Length + 1 + revisionKeys.Count * RevisionKeyLength);
        sb.Append(conversationId).Append(Separator);
        foreach (var key in revisionKeys)
            sb.Append(key);
        return sb.ToString();
    }

    public static SnapshotToken Parse(string token)
    {
        if (string.IsNullOrEmpty(token))
            throw new InvalidOperationException("Snapshot token is empty.");

        ReadOnlySpan<char> span = token;
        var separator = span.LastIndexOf(Separator);
        var keys = span[(separator + 1)..];

        // every key is 24 bytes, a whole number of base64 groups, so the keys are valid if their concatenation is
        if (separator <= 0 || keys.IsEmpty || keys.Length % RevisionKeyLength != 0 || Base64Url.IsValid(keys) == false)
            throw new InvalidOperationException($"Invalid snapshot token format: '{(token.Length > 200 ? token[..200] + "..." : token)}'");

        return new SnapshotToken(token, separator);
    }
}
