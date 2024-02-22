using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
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
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options { Server = server,DeleteDatabaseOnDispose = false, RunInMemory = false }))
            {
                Cluster.SuspendObserver(server);

                var testingStuff = server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly();
                testingStuff.BeforeHandleClusterTransactionOnDatabaseChanged = (s) => s.DatabasesLandlord.RestartDatabase(store.Database).WithCancellation(cts.Token);
                testingStuff.DelayNotifyFeaturesAboutStateChange = () => Thread.Sleep(1000);

                var user1 = new User { Name = "Karmel" };
                var user2 = new User { Name = "Karmel2" };
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(user1, "users/1", cts.Token);
                    await session.StoreAsync(user2, "users/2", cts.Token);

                    await Assert.ThrowsAsync<DatabaseDisabledException>(async () => await session.SaveChangesAsync(cts.Token));

                    Assert.True(await WaitForValueAsync(() => server.ServerStore.DatabasesLandlord.IsDatabaseLoaded(store.Database),
                        true, 10_000));

                    var database = await GetDatabase(server, store.Database);
                    var key = AlertRaised.GetKey(AlertType.ClusterTransactionFailure, $"{database.Name}/ClusterTransaction");
                    Assert.False(database.NotificationCenter.Exists(key));
                }
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var user = await session.LoadAsync<User>("users/1", cts.Token);
                    Assert.Equal(user1.Name, user.Name);

                    user = await session.LoadAsync<User>("users/2", cts.Token);
                    Assert.Equal(user2.Name, user.Name);
                }
            }
        }
    }
}
