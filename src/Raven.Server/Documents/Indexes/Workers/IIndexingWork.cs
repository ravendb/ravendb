using System;
using System.Threading;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Workers
{
    public interface IIndexingWork
    {
        string Name { get; }

        (bool MoreWorkFound, Index.CanContinueBatchResult BatchContinuationResult) Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext,
                     Lazy<IndexWriteOperationBase> writeOperation, IndexingStatsScope stats, CancellationToken token);
    }
}
