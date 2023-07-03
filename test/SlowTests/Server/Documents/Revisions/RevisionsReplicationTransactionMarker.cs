//-----------------------------------------------------------------------
// <copyright file="Revisions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading.Tasks;
using FastTests.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Revisions
{
    public class RevisionsReplicationTransactionMarker : ReplicationTestBase
    {
        public RevisionsReplicationTransactionMarker(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RealSupportForTransactionMarkerAcrossMultiUpdates(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var replication = await GetReplicationManagerAsync(store1, store1.Database, options.DatabaseMode, breakReplication: true))
                {
                    await RevisionsHelper.SetupRevisionsAsync(store1, modifyConfiguration: configuration =>
                    {
                        configuration.Collections["Users"].PurgeOnDelete = false;
                    });
                    await RevisionsHelper.SetupRevisionsAsync(store2, modifyConfiguration: configuration =>
                    {
                        configuration.Collections["Users"].PurgeOnDelete = false;
                    });

                    var id1 = "users/oren";
                    var id2 = $"users/fitzchak${id1}";
                    var id3 = $"users/michael${id1}";

                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Id = id1, Name = "Oren", Balance = 10 });
                        await session.StoreAsync(new User { Id = id2, Name = "Fitzchak", Balance = 10 });
                        await session.StoreAsync(new User { Id = id3, Name = "Michael", Balance = 10 });
                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        var fitzchak = await session.LoadAsync<User>(id2);
                        var michael = await session.LoadAsync<User>(id3);
                        michael.Balance -= 10;
                        fitzchak.Balance += 10;
                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        var fitzchak = await session.LoadAsync<User>(id2);
                        var oren = await session.LoadAsync<User>(id1);
                        fitzchak.Balance -= 5;
                        oren.Balance += 5;
                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        var michael = await session.LoadAsync<User>(id3);
                        session.Delete(michael);
                        var fitzchak = await session.LoadAsync<User>(id2);
                        var oren = await session.LoadAsync<User>(id1);
                        fitzchak.Balance -= 5;
                        oren.Balance += 5;
                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Id = id3, Name = "Michael", Balance = 10 });
                        var oren = await session.LoadAsync<User>(id1);
                        oren.Balance -= 10;
                        await session.SaveChangesAsync();
                    }

                    await SetupReplicationAsync(store1, store2);
                    replication.ReplicateOnce(id1);

                    using (var session = store2.OpenAsyncSession())
                    {
                        Assert.True(WaitForDocument<User>(store2, id3, u => u.Balance == 10));
                        var fitzchak = await session.LoadAsync<User>(id2);
                        var oren = await session.LoadAsync<User>(id1);
                        Assert.Equal(10, fitzchak.Balance);
                        Assert.Equal(10, oren.Balance);
                    }

                    replication.ReplicateOnce(id1);

                    using (var session = store2.OpenAsyncSession())
                    {
                        Assert.True(WaitForDocument<User>(store2, id3, u => u.Balance == 0));
                        var fitzchak = await session.LoadAsync<User>(id2);
                        var oren = await session.LoadAsync<User>(id1);
                        Assert.Equal(20, fitzchak.Balance);
                        Assert.Equal(10, oren.Balance);
                    }

                    replication.ReplicateOnce(id1);

                    using (var session = store2.OpenAsyncSession())
                    {
                        Assert.True(WaitForDocument<User>(store2, id1, u => u.Balance == 15));
                        var fitzchak = await session.LoadAsync<User>(id2);
                        var michael = await session.LoadAsync<User>(id3);
                        Assert.Equal(15, fitzchak.Balance);
                        Assert.Equal(0, michael.Balance);
                    }

                    replication.ReplicateOnce(id1);

                    using (var session = store2.OpenAsyncSession())
                    {
                        Assert.True(WaitForDocument<User>(store2, id1, u => u.Balance == 20));
                        var fitzchak = await session.LoadAsync<User>(id2);
                        var michael = await session.LoadAsync<User>(id3);
                        Assert.Equal(10, fitzchak.Balance);
                        Assert.Null(michael);
                    }

                    replication.ReplicateOnce(id1);

                    using (var session = store2.OpenAsyncSession())
                    {
                        Assert.True(WaitForDocument<User>(store2, id1, u => u.Balance == 10));
                        var fitzchak = await session.LoadAsync<User>(id2);
                        var michael = await session.LoadAsync<User>(id3);
                        Assert.Equal(10, fitzchak.Balance);
                        Assert.Equal(10, michael.Balance);
                    }
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
