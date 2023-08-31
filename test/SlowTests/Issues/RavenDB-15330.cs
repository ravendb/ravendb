using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;
public class RavenDB_15330 : ClusterTestBase
{
    public RavenDB_15330(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Command_With_DontCareId_Should_Be_Committed()
    {
        var typeProp = nameof(RachisLogHistory.LogHistoryColumn.Type);
        var stateProp = nameof(RachisLogHistory.LogHistoryColumn.State);
        var indexProp = nameof(RachisLogHistory.LogHistoryColumn.Index);

        var server = GetNewServer();
        using var store = GetDocumentStoreForRollingIndexes(new RavenTestBase.Options
        {
            ModifyDatabaseRecord =
                record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MaxTimeToWaitAfterFlushAndSyncWhenReplacingSideBySideIndex)] = "5",
            Server = server,
            ReplicationFactor = 1,
        });

        await store.ExecuteIndexAsync(new SomeRollingIndex());

        var result = await server.ServerStore.SendToLeaderAsync(new PutRollingIndexCommand(store.Database, nameof(SomeRollingIndex),
            DateTime.UtcNow, RaftIdGenerator.DontCareId));

        using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            var historyLog = server.ServerStore.Engine.LogHistory.GetHistoryLogs(context);
            bool contain = false;
            bool first = true;
            long lastIndex = 0;
            foreach (var djv in historyLog)
            {
                if (first)
                {
                    lastIndex = long.Parse(djv[indexProp].ToString());
                    first = false;
                }
                else
                {
                    var currentIndex = long.Parse(djv[indexProp].ToString());
                    Assert.Equal(lastIndex+1, currentIndex);
                    lastIndex = currentIndex;
                }

                if (djv[typeProp] != null && djv[typeProp].ToString() == nameof(PutRollingIndexCommand) && 
                    djv[stateProp] !=null && djv[stateProp].ToString() == "Committed")
                {
                    contain = true;
                    break;
                }
            }
            Assert.True(contain);
        }
    }

    private class SomeRollingIndex : AbstractIndexCreationTask<Order>
    {
        public SomeRollingIndex()
        {
            Map = orders => from order in orders
                select new
                {
                    order.Company,
                };

            DeploymentMode = IndexDeploymentMode.Rolling;
        }
    }

}

