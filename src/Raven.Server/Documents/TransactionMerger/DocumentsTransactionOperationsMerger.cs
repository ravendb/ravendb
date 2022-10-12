using System;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger;

public class DocumentsTransactionOperationsMerger : AbstractTransactionOperationsMerger<DocumentsOperationContext, DocumentsTransaction>
{
    private readonly DocumentDatabase _database;

    public DocumentsTransactionOperationsMerger([NotNull] DocumentDatabase database)
        : base(database.Name, database.Configuration, database.Time, database.DatabaseShutdown)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        IsEncrypted = database.IsEncrypted;
        Is32Bits = database.Is32Bits;
    }

    protected override bool IsEncrypted { get; }

    protected override bool Is32Bits { get; }

    internal override DocumentsTransaction BeginAsyncCommitAndStartNewTransaction(DocumentsTransaction previousTransaction, DocumentsOperationContext currentContext)
    {
        return previousTransaction.BeginAsyncCommitAndStartNewTransaction(currentContext);
    }

    internal override void UpdateGlobalReplicationInfoBeforeCommit(DocumentsOperationContext context)
    {
        if (string.IsNullOrEmpty(context.LastDatabaseChangeVector) == false)
        {
            _database.DocumentsStorage.SetDatabaseChangeVector(context, context.LastDatabaseChangeVector);
        }

        if (context.LastReplicationEtagFrom != null)
        {
            foreach (var repEtag in context.LastReplicationEtagFrom)
            {
                DocumentsStorage.SetLastReplicatedEtagFrom(context, repEtag.Key, repEtag.Value);
            }
        }
    }

    protected override void UpdateLastAccessTime(DateTime time)
    {
        _database.LastAccessTime = time;
    }
}
