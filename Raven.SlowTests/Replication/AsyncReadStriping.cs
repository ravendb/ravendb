using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Replication;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Tests.Common;
using Raven.Tests.Document;

using Xunit;

namespace Raven.SlowTests.Replication
{
    public class AsyncReadStriping : ReplicationBase
    {
        [Fact]
        public async Task When_replicating_can_do_read_striping()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();
            var store3 = CreateStore();

            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(new Company());
                await session.SaveChangesAsync();
            }

            SetupReplication(store1.DatabaseCommands, store2, store3);

            WaitForDocument(store2.DatabaseCommands, "companies/1");
            WaitForDocument(store3.DatabaseCommands, "companies/1");

            await PauseReplicationAsync(0, store1.DefaultDatabase);
            await PauseReplicationAsync(1, store2.DefaultDatabase);
            await PauseReplicationAsync(2, store3.DefaultDatabase);

            using(var store = new DocumentStore
            {
                Url = store1.Url,
                Conventions =
                    {
                        FailoverBehavior = FailoverBehavior.ReadFromAllServers
                    },
                    DefaultDatabase = store1.DefaultDatabase
            })
            {
                store.Initialize();
                var replicationInformerForDatabase = store.GetReplicationInformerForDatabase();
                await replicationInformerForDatabase.UpdateReplicationInformationIfNeededAsync((AsyncServerClient)store.AsyncDatabaseCommands);
                Assert.Equal(2, replicationInformerForDatabase.ReplicationDestinationsUrls.Count);

                foreach (var ravenDbServer in servers)
                {
                    ravenDbServer.Server.ResetNumberOfRequests();
                    Assert.Equal(0, ravenDbServer.Server.NumberOfRequests);
                }

                for (int i = 0; i < 6; i++)
                {
                    using(var session = store.OpenAsyncSession())
                    {
                        Assert.NotNull(await session.LoadAsync<Company>("companies/1"));
                    }
                }
            }

            foreach (var ravenDbServer in servers)
            {
                Assert.True(2 == ravenDbServer.Server.NumberOfRequests, string.Format("Server at port: {0}. Requests: #{1}", ravenDbServer.SystemDatabase.Configuration.Port, ravenDbServer.Server.NumberOfRequests));
            }
            
        }

        [Fact]
        public async Task Can_avoid_read_striping()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();
            var store3 = CreateStore();

            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(new Company());
                await session.SaveChangesAsync();
            }

            SetupReplication(store1.DatabaseCommands, store2, store3);

            WaitForDocument(store2.DatabaseCommands, "companies/1");
            WaitForDocument(store3.DatabaseCommands, "companies/1");

            await PauseReplicationAsync(0, store1.DefaultDatabase);
            await PauseReplicationAsync(1, store2.DefaultDatabase);
            await PauseReplicationAsync(2, store3.DefaultDatabase);

            using (var store = new DocumentStore
            {
                Url = store1.Url,
                Conventions =
                {
                    FailoverBehavior = FailoverBehavior.ReadFromAllServers
                },
                DefaultDatabase = store1.DefaultDatabase
            })
            {
                store.Initialize();
                var replicationInformerForDatabase = store.GetReplicationInformerForDatabase();
                await replicationInformerForDatabase.UpdateReplicationInformationIfNeededAsync((AsyncServerClient)store.AsyncDatabaseCommands);
                Assert.Equal(2, replicationInformerForDatabase.ReplicationDestinations.Count);

                foreach (var ravenDbServer in servers)
                {
                    ravenDbServer.Server.ResetNumberOfRequests();
                }

                for (int i = 0; i < 6; i++)
                {
                    using (var session = store.OpenAsyncSession(new OpenSessionOptions
                    {
                        ForceReadFromMaster = true
                    }))
                    {
                        Assert.NotNull(await session.LoadAsync<Company>("companies/1"));
                    }
                }
            }
            Assert.Equal(6, servers[0].Server.NumberOfRequests);
            Assert.Equal(0, servers[1].Server.NumberOfRequests);
            Assert.Equal(0, servers[2].Server.NumberOfRequests);
        }
    }
}
