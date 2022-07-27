using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Documents.Revisions;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Index = SlowTests.Issues.Index;

namespace SlowTests.Cluster
{
    // tests for RavenDB-13304
    public class ClusterIndexNotificationsTest : ClusterTestBase
    {
        public ClusterIndexNotificationsTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task NotifyAfterServerRestart()
        {
            using (var store = GetDocumentStore(new Options { RunInMemory = false }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);

                    await session.StoreAsync(new User
                    {
                        Name = "karmel"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                var old = await Databases.GetDocumentDatabaseInstanceFor(store);
                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.NotNull(user);
                }
            }
        }

        [Fact]
        public async Task ShouldWaitForIndexOfClusterSideEffects()
        {
            using (var store = GetDocumentStore(new Options { DeleteDatabaseOnDispose = true, Path = NewDataPath() }))
            using (var store2 = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("likes");
                    await session.SaveChangesAsync();
                }

                var documentDatabase = await GetDatabase(store.Database);
                var testingStuff = documentDatabase.ForTestingPurposesOnly();

                using (testingStuff.CallDuringDocumentDatabaseInternalDispose(() =>
                {
                    Thread.Sleep(12345);
                }))
                {
                    using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
                    var task = BackgroundWorkAsync(store2, cts);

                    await WaitForIndexCreationAsync(store2, cts.Token);

                    await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, true), cts.Token);
                    await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, false), cts.Token);

                    using (var session = store.OpenAsyncSession())
                    {
                        session.CountersFor("users/1").Increment("likes");
                        await session.SaveChangesAsync(cts.Token);
                    }

                    cts.Cancel();

                    try
                    {
                        task.Wait(TimeSpan.FromSeconds(60));
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }
        }

        [Fact]
        public async Task RavenDB_14086()
        {
            using (var store = GetDocumentStore())
            {
                var indexes = new List<Task>();
                for (int i = 0; i < 100; i++)
                {
                    var index = new Index($"test{i}");
                    indexes.Add(index.ExecuteAsync(store));
                }

                await Task.WhenAll(indexes);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(100, record.Indexes.Count);
            }
        }

        internal static async Task WaitForIndexCreationAsync(DocumentStore store, CancellationToken token)
        {
            while ((await store.Maintenance.SendAsync(new GetStatisticsOperation(), token)).CountOfIndexes == 0)
            {
                await Task.Delay(1000, token);
            }
        }

        internal static async Task BackgroundWorkAsync(DocumentStore store2, CancellationTokenSource cts)
        {
            while (cts.IsCancellationRequested == false)
            {
                await new UsersIndex().ExecuteAsync(store2, token: cts.Token);
                await Task.Delay(1000, cts.Token);
            }
        }

        private class UsersIndex : AbstractIndexCreationTask<User>
        {
            public override string IndexName => Guid.NewGuid().ToString();

            public UsersIndex()
            {
                Map = users =>
                    from user in users
                    select new
                    {
                        user.AddressId
                    };
            }
        }
    }
}
