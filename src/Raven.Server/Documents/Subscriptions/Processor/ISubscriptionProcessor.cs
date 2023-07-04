using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Subscriptions.Processor;

public interface ISubscriptionProcessor<TIncludesCommand> : IDisposable
    where TIncludesCommand : AbstractIncludesCommand
{
    IDisposable InitializeForNewBatch(ClusterOperationContext clusterContext, out TIncludesCommand includesCommands);
    Task<SubscriptionBatchResult> GetBatchAsync(SubscriptionBatchStatsScope batchScope, Stopwatch sendingCurrentBatchStopwatch);
    Task<long> RecordBatchAsync(string lastChangeVectorSentInThisBatch);
    Task AcknowledgeBatchAsync(long currentBatchId, string clientReplyChangeVector);
}
