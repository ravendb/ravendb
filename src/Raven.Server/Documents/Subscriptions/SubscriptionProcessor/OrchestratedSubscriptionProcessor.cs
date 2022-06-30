using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.ServerWide;
using Sparrow.Utils;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor;

public class OrchestratedSubscriptionProcessor : SubscriptionProcessor
{
    private readonly ShardedDatabaseContext _databaseContext;
    private SubscriptionConnectionsStateOrchestrator _state;

    public ShardedSubscriptionBatch CurrentBatch;

    public OrchestratedSubscriptionProcessor(ServerStore server, ShardedDatabaseContext databaseContext, SubscriptionConnectionBase connection) : base(server, connection)
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

    protected override SubscriptionIncludeCommands CreateIncludeCommands()
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Major,
            "https://issues.hibernatingrhinos.com/issue/RavenDB-16279");
        return new SubscriptionIncludeCommands();
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
        }
    }
}
