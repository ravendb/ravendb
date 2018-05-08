using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Cluster
{
    public class ClusterTransactionTests : ClusterTestBase
    {
        [Fact]
        public async Task CanCreateClusterTransactionRequest()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var db = GetDatabaseName();
            await CreateDatabaseInCluster(db, 3, leader.WebUrl);
            using (var leaderStore = new DocumentStore()
            {
                Urls = new []{leader.WebUrl},
                Database = db,
            }.Initialize())
            {
                var user1 = new User()
                {
                    Name = "Karmel"
                };
                var user3 = new User()
                {
                    Name = "Indych"
                };
                
                using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                    await session.StoreAsync(user3, "foo/bar");
                    await session.SaveChangesAsync();

                    var user = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende")).Value;
                    Assert.Equal(user1.Name, user.Name);
                    user = await session.LoadAsync<User>("foo/bar");
                    Assert.Equal(user3.Name, user.Name);
                }
            }
        }

        public class UniqueUser
        {
            public string Id { get; set; }
            public string Email { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public async Task CreateUniqueUser()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var db = GetDatabaseName();
            await CreateDatabaseInCluster(db, 3, leader.WebUrl);
            using (var leaderStore = new DocumentStore()
            {
                Urls = new[] { leader.WebUrl },
                Database = db,
            }.Initialize())
            {
                var email = "grisha@ayende.com";
                var userId = $"users/{email}";

                var task1 = Task.Run(async () => await AddUser(leaderStore, email, userId));
                var task2 = Task.Run(async () => await AddUser(leaderStore, email, userId));
                var task3 = Task.Run(async() =>
                {
                    using (var session = leaderStore.OpenAsyncSession())
                    {
                        while (true)
                        {
                            var user = await session.LoadAsync<UniqueUser>(userId);
                            if (user != null)
                                break;

                            await Task.Delay(500);
                            if (task1.IsCompleted && task2.IsCompleted)
                                break;
                        }

                        var userNew = await session.LoadAsync<UniqueUser>(userId);
                        Assert.NotNull(userNew);
                    }
                });

                await Task.WhenAll(task1, task2, task3);

                Assert.True(task1.IsCompletedSuccessfully);
                Assert.True(task2.IsCompletedSuccessfully);
                Assert.True(task3.IsCompletedSuccessfully);
                Assert.Equal(1, task1.Result + task2.Result);
            }
        }

        private static async Task<int> AddUser(IDocumentStore leaderStore, string email, string userId)
        {
            try
            {
                using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                    {TransactionMode = TransactionMode.ClusterWide}))
                {
                    var user = new UniqueUser
                    {
                        Name = "Grisha",
                        Email = email
                    };
                    await session.StoreAsync(user, userId);
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(email, userId);

                    await session.SaveChangesAsync();
                }
            }
            catch(ConcurrencyException)
            {
                return 1;
            }

            return 0;
        }

        [Fact]
        public async Task SessionCompareExchangeCommands()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                var user1 = new User()
                {
                    Name = "Karmel"
                };
                var user3 = new User()
                {
                    Name = "Indych"
                };

                session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/karmel", user1);
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/grisha", user1);
                await session.SaveChangesAsync();

                var user = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende"));
                Assert.Equal(user1.Name, user.Value.Name);

                var indexes = await session.Advanced.ClusterTransaction.GetCompareExchangeIndexesAsync(new [] { "usernames/ayende" , "usernames/karmel" , "usernames/grisha" });
                Assert.Equal(3, indexes.Count);
                Assert.Equal(user.Index, indexes["usernames/ayende"]);
            }
        }

        [Fact]
        public async Task ThrowOnUnsupportedOperations()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                session.Advanced.Attachments.Store("asd", "test", new MemoryStream(new byte[] {1, 2, 3, 4}));
                await Assert.ThrowsAsync<NotSupportedException>(async () => await session.SaveChangesAsync());
            }
        }

        [Fact]
        public async Task ThrowOnInvalidTransactionMode()
        {
            var user1 = new User()
            {
                Name = "Karmel"
            };
            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession())
            {
                Assert.Throws<InvalidOperationException>(() => session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1));
                Assert.Throws<InvalidOperationException>(() =>
                    session.Advanced.ClusterTransaction.UpdateCompareExchangeValue(new CompareExchangeValue<string>("test", 0, "test")));
                Assert.Throws<InvalidOperationException>(() => session.Advanced.ClusterTransaction.DeleteCompareExchangeValue("usernames/ayende", 0));
                await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende"));
            }
        }
    }
}
