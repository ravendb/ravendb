using System;
using System.Linq;
using System.Threading.Tasks;
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
        var server = GetNewServer();
        using var store = GetDocumentStoreForRollingIndexes(new Options
        {
            ModifyDatabaseRecord =
                record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MaxTimeToWaitAfterFlushAndSyncWhenReplacingSideBySideIndex)] = "5",
            Server = server,
            ReplicationFactor = 1,
        });

        await server.ServerStore.SendToLeaderAsync(new PutRollingIndexCommand(store.Database, "SomeRollingIndex",
            DateTime.UtcNow, RaftIdGenerator.DontCareId));

        using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            var historyLog = server.ServerStore.Engine.LogHistory.GetHistoryLogs(context);
            bool contain = historyLog.Any(djv => djv[nameof(RachisLogHistory.LogHistoryColumn.Type)]?.ToString() == nameof(PutRollingIndexCommand) && 
                                                 djv[nameof(RachisLogHistory.LogHistoryColumn.State)]?.ToString() == "Committed");
            Assert.True(contain);
        }
    }
}

