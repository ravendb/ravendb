using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor;

public class OrchestratedSubscriptionProcessor : AbstractSubscriptionProcessor<OrchestratorIncludesCommandImpl>
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
        _state = _databaseContext.SubscriptionsStorage.Subscriptions[Connection.SubscriptionId];
    }

    // should never hit this
    public override Task<long> RecordBatch(string lastChangeVectorSentInThisBatch) => throw new NotImplementedException();

    // should never hit this
    public override Task AcknowledgeBatch(long batchId) => throw new NotImplementedException();

    private OrchestratorIncludesCommandImpl _includes;

    protected override OrchestratorIncludesCommandImpl CreateIncludeCommands()
    {
        var includeDocuments = new IncludeDocumentsOrchestratedSubscriptionCommand(ClusterContext, _state.CancellationTokenSource.Token);
        var includeCounters = new ShardedCounterIncludes(_state.CancellationTokenSource.Token);
        var includeTimeSeries = new ShardedTimeSeriesIncludes(supportsMissingIncludes: false, _state.CancellationTokenSource.Token);

        _includes = new OrchestratorIncludesCommandImpl(includeDocuments, includeTimeSeries, includeCounters);

        return _includes;
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

            CurrentBatch.CloneIncludes(ClusterContext, _includes);
        }
    }
}
