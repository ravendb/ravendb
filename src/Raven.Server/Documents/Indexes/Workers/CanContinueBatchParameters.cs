using System;
using System.Diagnostics;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Workers
{
    public readonly struct CanContinueBatchParameters
    {
        public CanContinueBatchParameters(IndexingStatsScope stats, IndexingWorkType workType, QueryOperationContext queryContext, TransactionOperationContext indexingContext,
            Lazy<IndexWriteOperationBase> indexWriteOperation, long currentEtag, long maxEtag, long count,
            Stopwatch sw)
        {
            Stats = stats;
            WorkType = workType;
            QueryContext = queryContext;
            IndexingContext = indexingContext;
            IndexWriteOperation = indexWriteOperation;
            CurrentEtag = currentEtag;
            MaxEtag = maxEtag;
            Count = count;
            Sw = sw;
        }

        public IndexingStatsScope Stats { get; }

        public IndexingWorkType WorkType { get; }

        public QueryOperationContext QueryContext { get; }

        public TransactionOperationContext IndexingContext { get; }

        public Lazy<IndexWriteOperationBase> IndexWriteOperation { get; }

        public long CurrentEtag { get; }

        public long MaxEtag { get; }

        public long Count { get; }

        public Stopwatch Sw { get; }

    }
}
