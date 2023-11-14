using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Database;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20708 : ReplicationTestBase
    {
        public RavenDB_20708(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public async Task ShouldNotBlockClusterTransactionOnDatabaseStartup()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore(new Options { DeleteDatabaseOnDispose = false, RunInMemory = false }))
            {
                Cluster.SuspendObserver(Server);

                var testingStuff = Server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly();
                testingStuff.BeforeHandleClusterTransactionOnDatabaseChanged = (server) => server.DatabasesLandlord.RestartDatabase(store.Database);
                testingStuff.DelayNotifyFeaturesAboutStateChange = () => Thread.Sleep(1000);

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var user1 = new User { Name = "Karmel" };
                    var user2 = new User { Name = "Karmel2" };

                    await session.StoreAsync(user1, "users/1");
                    await session.StoreAsync(user2, "users/2");

                    await Assert.ThrowsAsync<DatabaseDisabledException>(async () => await session.SaveChangesAsync());

                    Assert.True(await WaitForValueAsync(() => Server.ServerStore.DatabasesLandlord.IsDatabaseLoaded(store.Database),
                        true, 10_000));

                    var database = await GetDatabase(store.Database);
                    var key = AlertRaised.GetKey(AlertType.ClusterTransactionFailure, $"{database.Name}/ClusterTransaction");
                    Assert.False(database.NotificationCenter.Exists(key));

                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal(user1.Name, user.Name);

                    user = await session.LoadAsync<User>("users/2");
                    Assert.Equal(user2.Name, user.Name);
                }
            }
        }
    }
}
