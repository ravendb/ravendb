using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class UpdateTopologyTests : ClusterTestBase
    {
        public UpdateTopologyTests(ITestOutputHelper output) : base(output)
        {
        }
        [Fact]
        public async Task CanUpdateTopologyDuringNodeDeletion()
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges();

                    var entity = new User();
                    await session.StoreAsync(entity);
                    await session.SaveChangesAsync();
                }

                var serverA = nodes.Single(n => n.ServerStore.NodeTag == "A");
                var mre = new ManualResetEventSlim(false);
                try
                {
                    serverA.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().BeforeActualDelete = () => mre.Wait(TimeSpan.FromSeconds(30));

                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true, "A", timeToWaitForConfirmation: null));

                    await store.GetRequestExecutor().UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(new ServerNode { Url = leader.WebUrl, Database = store.Database }));
                }
                finally
                {
                    mre.Set(); 
                }
            }
        }
    }
}
