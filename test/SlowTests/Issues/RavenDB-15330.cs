using System;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands.Indexes;
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
        using var store = GetDocumentStoreForRollingIndexes(new Options
        {
            ModifyDatabaseRecord =
                record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MaxTimeToWaitAfterFlushAndSyncWhenReplacingSideBySideIndex)] = "5",
            ReplicationFactor = 1,
            Server = Server
        });

        await Server.ServerStore.SendToLeaderAsync(new PutRollingIndexCommand(store.Database, "SomeRollingIndex",
            DateTime.UtcNow, RaftIdGenerator.DontCareId));

        await Cluster.WaitForRaftCommandToBeAppliedInLocalServerAsync(nameof(PutRollingIndexCommand));
    }
}

