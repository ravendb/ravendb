using System;
using JetBrains.Annotations;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.TransactionMerger;

public sealed class DocumentsTransactionOperationsMerger : AbstractTransactionOperationsMerger<DocumentsOperationContext, DocumentsTransaction>
{
    private readonly DocumentDatabase _database;

    public DocumentsTransactionOperationsMerger([NotNull] DocumentDatabase database)
        : base(database.Name, database.Configuration, database.Time, RavenLogManager.Instance.GetLoggerForDatabase<DocumentsTransactionOperationsMerger>(database), database.DatabaseShutdown)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

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
