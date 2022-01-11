using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide;
using Sparrow.Server.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17231 : ClusterTestBase
    {
        public RavenDB_17231(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task FailuresDuringEmptyQueueShouldCauseReelection()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            var dbName = GetDatabaseName();
            var db = await CreateDatabaseInCluster(dbName, 2, leader.WebUrl);

            var cmdCount = 0;
            leader.ServerStore.Engine.BeforeAppendToRaftLog += (context, cmd) =>
            {
                if (++cmdCount > 10)
                {
                    throw new DiskFullException("no more space");
                }
            };

            var followers = nodes.Where(s => s != leader).ToList();
            var leaderSteppedDown = leader.ServerStore.Engine.WaitForLeaveState(RachisState.Leader, CancellationToken.None);
            var newLeaderElected = Task.WhenAny(followers.Select(s => s.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None)));
            var putConnectionStrings = Task.Run(async () =>
            {
                using (var store = new DocumentStore
                {
                    Database = dbName,
                    Urls = db.Servers.Select(s => s.WebUrl).ToArray()
                }.Initialize())
                {
                    var urls = nodes.Select(s => s.WebUrl).ToArray();
                    for (int i = 0; i < 20; i++)
                    {
                        var cs = new RavenConnectionString
                        {
                            Database = $"db/{i}",
                            Name = $"cs/{i}",
                            TopologyDiscoveryUrls = urls
                        };

                        await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(cs));
                    }
                }
            });

            Assert.True(leaderSteppedDown.Wait(TimeSpan.FromSeconds(15)));
            Assert.True(newLeaderElected.Wait(TimeSpan.FromSeconds(15)));

            await putConnectionStrings;
        }
    }
}
