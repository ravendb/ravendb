using System;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Subscriptions.Processor;

public abstract class AbstractSubscriptionProcessor<TIncludesCommand, TItem> : ISubscriptionProcessor<TIncludesCommand>
    where TIncludesCommand : AbstractIncludesCommand
{
    protected readonly ServerStore Server;
    protected readonly ISubscriptionConnection Connection;

    protected EndPoint RemoteEndpoint;
    protected SubscriptionState SubscriptionState;
    protected SubscriptionWorkerOptions Options;
    protected string Collection;

    protected int BatchSize => Options.MaxDocsPerBatch;

    protected RavenLogger Logger;

    protected AbstractSubscriptionProcessor(ServerStore server, ISubscriptionConnection connection, string databaseName)
    {
        Server = server;
        Connection = connection;
        Logger = RavenLogManager.Instance.GetLoggerForDatabase(GetType(), databaseName);
    }

    public virtual void InitializeProcessor()
    {
        Collection = Connection.Subscription.Collection;
        Options = Connection.Options;
        SubscriptionState = Connection.SubscriptionState;
        RemoteEndpoint = Connection.TcpConnection.TcpClient.Client.RemoteEndPoint;
    }

    protected virtual bool CanContinueBatch(SubscriptionBatchItemStatus batchItemStatus, SubscriptionBatchStatsScope batchScope, int numberOfDocs, Stopwatch sendingCurrentBatchStopwatch)
    {
        if (Connection.CancellationTokenSource.Token.IsCancellationRequested)
            return false;

        return true;
    }

    protected virtual async ValueTask SendHeartbeatIfNeededAsync(Stopwatch sendingCurrentBatchStopwatch)
    {
        if (sendingCurrentBatchStopwatch.Elapsed >= ISubscriptionConnection.HeartbeatTimeout)
        {
            // in v6.0 we don't use FlushBatchIfNeededAsync any more, we will send heartbeats each 3 sec even if we are not skipping docs
            await Connection.SendHeartBeatAsync($"Skipping docs for more than '{ISubscriptionConnection.HeartbeatTimeout.TotalMilliseconds}' ms without sending any data");
            sendingCurrentBatchStopwatch.Restart();
        }
    }

    protected virtual SubscriptionBatchStatus SetBatchStatus(SubscriptionBatchResult result)
    {
        return result.CurrentBatch.Count > 0 ? SubscriptionBatchStatus.DocumentsSent : SubscriptionBatchStatus.EmptyBatch;
    }

    public abstract Task<SubscriptionBatchResult> GetBatchAsync(SubscriptionBatchStatsScope batchScope, Stopwatch sendingCurrentBatchStopwatch);

    protected abstract string SetLastChangeVectorInThisBatch(IChangeVectorOperationContext context, string currentLast, SubscriptionBatchItem batchItem);

    public abstract Task<long> TryRecordBatchAsync(string lastChangeVectorSentInThisBatch);

    public abstract Task AcknowledgeBatchAsync(long batchId, string changeVector);

    protected ClusterOperationContext ClusterContext;
    protected TIncludesCommand IncludesCmd;

    public virtual IDisposable InitializeForNewBatch(ClusterOperationContext clusterContext, out TIncludesCommand includesCommands)
    {
        ClusterContext = clusterContext;
        InitializeProcessor();

        var commands = CreateIncludeCommands();
        IncludesCmd = commands;
        includesCommands = commands;

        return null;
    }

    protected abstract TIncludesCommand CreateIncludeCommands();

    protected abstract ConflictStatus GetConflictStatus(string changeVector);

    protected abstract void HandleBatchItem(SubscriptionBatchStatsScope batchScope, SubscriptionBatchItem batchItem, SubscriptionBatchResult result, TItem item);

    protected abstract SubscriptionBatchItem GetBatchItem(TItem item);

    public virtual void Dispose()
    {
    }
}
