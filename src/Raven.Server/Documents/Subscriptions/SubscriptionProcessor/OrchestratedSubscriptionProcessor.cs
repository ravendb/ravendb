﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor;

public class OrchestratedSubscriptionProcessor : AbstractSubscriptionProcessor<OrchestratorIncludesCommandImpl>
{
    private readonly ShardedDatabaseContext _databaseContext;
    private SubscriptionConnectionsStateOrchestrator _state;
    private CancellationToken _token;
    public ShardedSubscriptionBatch CurrentBatch;

    public OrchestratedSubscriptionProcessor(ServerStore server, ShardedDatabaseContext databaseContext, OrchestratedSubscriptionConnection connection) : base(server, connection, connection.DatabaseName)
    {
        _databaseContext = databaseContext;
        _token = connection.CancellationTokenSource.Token;
    }

    public override void InitializeProcessor()
    {
        base.InitializeProcessor();
        _state = _databaseContext.SubscriptionsStorage.Subscriptions[Connection.SubscriptionId];
        Connection.SubscriptionState = _databaseContext.SubscriptionsStorage.GetSubscriptionById(ClusterContext, Connection.SubscriptionId);
    }

    // should never hit this
    public override Task<long> RecordBatchAsync(string lastChangeVectorSentInThisBatch) => throw new NotImplementedException();

    // should never hit this
    public override Task AcknowledgeBatchAsync(long batchId, string changeVector) => throw new NotImplementedException();

    private OrchestratorIncludesCommandImpl _includes;

    protected override string SetLastChangeVectorInThisBatch(IChangeVectorOperationContext context, string currentLast, BatchItem batchItem)
    {
        return batchItem.Document.ChangeVector;
    }

    protected override OrchestratorIncludesCommandImpl CreateIncludeCommands()
    {
        var includeDocuments = new IncludeDocumentsOrchestratedSubscriptionCommand(ClusterContext, _state.CancellationTokenSource.Token);
        var includeCounters = new ShardedCounterIncludes(_state.CancellationTokenSource.Token);
        var includeTimeSeries = new ShardedTimeSeriesIncludes(supportsMissingIncludes: false, _state.CancellationTokenSource.Token);

        _includes = new OrchestratorIncludesCommandImpl(includeDocuments, includeTimeSeries, includeCounters);

        return _includes;
    }

    protected override ConflictStatus GetConflictStatus(string changeVector)
    {
        var vector = ClusterContext.GetChangeVector(changeVector);

        var conflictStatus = ChangeVectorUtils.GetConflictStatus(
            remoteAsString: vector.Order,
            localAsString: _state.LastChangeVectorSent);
        return conflictStatus;
    }

    public override async Task<SubscriptionBatchResult> GetBatchAsync(SubscriptionBatchStatsScope batchScope, Stopwatch sendingCurrentBatchStopwatch)
    {
        var result = new SubscriptionBatchResult { CurrentBatch = new List<BatchItem>(), LastChangeVectorSentInThisBatch = null };
        if (_state.Batches.TryTake(out CurrentBatch, TimeSpan.Zero) == false)
        {
            result.Status = BatchStatus.EmptyBatch;
            return result;
        }

        using (CurrentBatch.ReturnContext)
        {
            foreach (SubscriptionBatchBase<BlittableJsonReaderObject>.Item item in CurrentBatch.Items)
            {
                BatchItem batchItem = GetBatchItem(item);
                if (batchItem.Status == BatchItemStatus.Skip)
                {
                    continue;
                }

                HandleBatchItem(batchScope: null, batchItem, result, item);
                if (await CanContinueBatchAsync(batchItem, size: default, result.CurrentBatch.Count, sendingCurrentBatchStopwatch) == false)
                    break;
            }

            CurrentBatch.CloneIncludes(ClusterContext, _includes);

            _token.ThrowIfCancellationRequested();
            result.Status = SetBatchStatus(result);
            return result;
        }
    }

    protected void HandleBatchItem(SubscriptionBatchStatsScope batchScope, BatchItem batchItem, SubscriptionBatchResult result, SubscriptionBatchBase<BlittableJsonReaderObject>.Item item)
    {
        //TODO: egor abstract this with DatabaseSubscriptionProcessor<T>.HandleBatchItem()
        Connection.TcpConnection.LastEtagSent = batchItem.Document.Etag;
        result.CurrentBatch.Add(batchItem);
        result.LastChangeVectorSentInThisBatch = SetLastChangeVectorInThisBatch(ClusterContext, result.LastChangeVectorSentInThisBatch, batchItem);

    }

    private BatchItem GetBatchItem(SubscriptionBatchBase<BlittableJsonReaderObject>.Item item)
    {
        if (GetConflictStatus(item.ChangeVector) == ConflictStatus.AlreadyMerged)
        {
            return new BatchItem { Status = BatchItemStatus.Skip };
        }


        if (item.ExceptionMessage != null)
        {
            return new BatchItem { Exception = new Exception(item.ExceptionMessage) };
        }

        return new BatchItem
        {
            Document = new Document
            {
                Data = item.RawResult.Clone(ClusterContext), 
                ChangeVector = item.ChangeVector, 
                Id = ClusterContext.GetLazyString(item.Id)
            }
        };
    }
}
