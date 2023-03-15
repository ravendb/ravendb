using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using SlowTests.Rolling;
using Sparrow.Json.Parsing;
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
        DebuggerAttachedTimeout.DisableLongTimespan = true;
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
            foreach (var djv in historyLog)
            {
                if (djv!=null && 
                    djv["Type"] != null && djv["Type"].ToString() == nameof(PutRollingIndexCommand) && 
                    djv["State"]!=null && djv["State"].ToString() == "Committed")
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

