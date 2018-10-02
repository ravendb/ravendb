//-----------------------------------------------------------------------
// <copyright file="Revisions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading.Tasks;
using FastTests.Server.Documents.Revisions;
using FastTests.Server.Replication;
using Sparrow;
using Xunit;
using System.Threading;
using FastTests.Utils;

namespace SlowTests.Server.Documents.Revisions
{
    public class RevisionsReplicationTransactionMarker : ReplicationTestBase
    {
        [Fact]
        public async Task RealSupportForTransactionMarkerAcrossMultiUpdates()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var database1 = await GetDocumentDatabaseInstanceFor(store1);
                database1.Configuration.Replication.MaxItemsCount = 1;
                database1.ReplicationLoader.DebugWaitAndRunReplicationOnce = new ManualResetEventSlim();

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database, configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = false;
                });
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database, configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = false;
                });

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Id = "users/oren", Name = "Oren", Balance = 10 });
                    await session.StoreAsync(new User { Id = "users/fitzchak", Name = "Fitzchak", Balance = 10 });
                    await session.StoreAsync(new User { Id = "users/michael", Name = "Michael", Balance = 10 });
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var fitzchak = await session.LoadAsync<User>("users/fitzchak");
                    var michael = await session.LoadAsync<User>("users/michael");
                    michael.Balance -= 10;
                    fitzchak.Balance += 10;
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var fitzchak = await session.LoadAsync<User>("users/fitzchak");
                    var oren = await session.LoadAsync<User>("users/oren");
                    fitzchak.Balance -= 5;
                    oren.Balance += 5;
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var michael = await session.LoadAsync<User>("users/michael");
                    session.Delete(michael);
                    var fitzchak = await session.LoadAsync<User>("users/fitzchak");
                    var oren = await session.LoadAsync<User>("users/oren");
                    fitzchak.Balance -= 5;
                    oren.Balance += 5;
                    await session.SaveChangesAsync();
                }


                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Id = "users/michael", Name = "Michael", Balance = 10 });
                    var oren = await session.LoadAsync<User>("users/oren");
                    oren.Balance -= 10;
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);

                database1.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();
                using (var session = store2.OpenAsyncSession())
                {
                    Assert.True(WaitForDocument<User>(store2, "users/michael", u => u.Balance == 10));
                    var fitzchak = await session.LoadAsync<User>("users/fitzchak");
                    var oren = await session.LoadAsync<User>("users/oren");
                    Assert.Equal(10, fitzchak.Balance);
                    Assert.Equal(10, oren.Balance);
                }

                database1.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();
                using (var session = store2.OpenAsyncSession())
                {
                    Assert.True(WaitForDocument<User>(store2, "users/michael", u => u.Balance == 0));
                    var fitzchak = await session.LoadAsync<User>("users/fitzchak");
                    var oren = await session.LoadAsync<User>("users/oren");
                    Assert.Equal(20, fitzchak.Balance);
                    Assert.Equal(10, oren.Balance);
                }

                database1.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();
                using (var session = store2.OpenAsyncSession())
                {
                    Assert.True(WaitForDocument<User>(store2, "users/oren", u => u.Balance == 15));
                    var fitzchak = await session.LoadAsync<User>("users/fitzchak");
                    var michael = await session.LoadAsync<User>("users/michael");
                    Assert.Equal(15, fitzchak.Balance);
                    Assert.Equal(0, michael.Balance);
                }

                database1.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();
                using (var session = store2.OpenAsyncSession())
                {
                    Assert.True(WaitForDocument<User>(store2, "users/oren", u => u.Balance == 20));
                    var fitzchak = await session.LoadAsync<User>("users/fitzchak");
                    var michael = await session.LoadAsync<User>("users/michael");
                    Assert.Equal(10, fitzchak.Balance);
                    Assert.Null(michael);
                }

                database1.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();
                using (var session = store2.OpenAsyncSession())
                {
                    Assert.True(WaitForDocument<User>(store2, "users/oren", u => u.Balance == 10));
                    var fitzchak = await session.LoadAsync<User>("users/fitzchak");
                    var michael = await session.LoadAsync<User>("users/michael");
                    Assert.Equal(10, fitzchak.Balance);
                    Assert.Equal(10, michael.Balance);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public decimal Balance { get; set; }
        }
    }
}
