using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor;

public class OrchestratedSubscriptionProcessor : AbstractSubscriptionProcessor<IncludeDocumentsOrchestratedSubscriptionCommand>
{
    private readonly ShardedDatabaseContext _databaseContext;
    private SubscriptionConnectionsStateOrchestrator _state;

    public ShardedSubscriptionBatch CurrentBatch;

    public OrchestratedSubscriptionProcessor(ServerStore server, ShardedDatabaseContext databaseContext, OrchestratedSubscriptionConnection connection) : base(server, connection, connection.DatabaseName)
    {
        _databaseContext = databaseContext;
    }

    public override void InitializeProcessor()
    {
        base.InitializeProcessor();
        _state = _databaseContext.Subscriptions.SubscriptionsConnectionsState[Connection.SubscriptionId];
    }

    // should never hit this
    public override Task<long> RecordBatch(string lastChangeVectorSentInThisBatch) => throw new NotImplementedException();

    // should never hit this
    public override Task AcknowledgeBatch(long batchId) => throw new NotImplementedException();

    private IncludeDocumentsOrchestratedSubscriptionCommand _includeDocuments;

    private ShardedCounterIncludes _includeCounters;

    private ShardedTimeSeriesIncludes _includeTimeSeries;

    protected override (IncludeDocumentsOrchestratedSubscriptionCommand IncludesCommand, ITimeSeriesIncludes TimeSeriesIncludes, ICounterIncludes CounterIncludes) CreateIncludeCommands()
    {
        _includeDocuments = new IncludeDocumentsOrchestratedSubscriptionCommand(ClusterContext, _state.CancellationTokenSource.Token);
        _includeCounters = new ShardedCounterIncludes(_state.CancellationTokenSource.Token);
        _includeTimeSeries = new ShardedTimeSeriesIncludes(supportsMissingIncludes: false, _state.CancellationTokenSource.Token);

        return (_includeDocuments, _includeTimeSeries, _includeCounters);
    }

    public override IEnumerable<(Document Doc, Exception Exception)> GetBatch()
    {
        if (_state.Batches.TryTake(out CurrentBatch, TimeSpan.Zero) == false)
            yield break;

        using (CurrentBatch.ReturnContext)
        {
            foreach (var batchItem in CurrentBatch.Items)
            {
                Connection.CancellationTokenSource.Token.ThrowIfCancellationRequested();

                if (batchItem.ExceptionMessage != null)
                    yield return (null, new Exception(batchItem.ExceptionMessage));

                var document = new Document
                {
                    Data = batchItem.RawResult.Clone(ClusterContext),
                    ChangeVector = batchItem.ChangeVector,
                    Id = ClusterContext.GetLazyString(batchItem.Id)
                };

                yield return (document, null);
            }

            // clone includes to the orchestrator context
            if (CurrentBatch._includes != null)
                _includeDocuments.Gather(CurrentBatch._includes);
            if (CurrentBatch._counterIncludes != null)
                _includeCounters.Gather(CurrentBatch._counterIncludes, ClusterContext);
            if (CurrentBatch._timeSeriesIncludes != null)
                _includeTimeSeries.Gather(CurrentBatch._timeSeriesIncludes, ClusterContext);
        }
    }
}
