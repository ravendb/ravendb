using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
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
                    await session.StoreAsync(user3,"foo/bar");
                    await session.SaveChangesAsync();

                    var user = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende")).Value;
                    Assert.Equal(user1.Name, user.Name);
                    user = await session.LoadAsync<User>("foo/bar");
                    Assert.Equal(user3.Name, user.Name);
                }
            }
        }

        [Fact]
        public void ThrowOnUnsupportedOperations()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                Assert.Throws<NotSupportedException>(() => session.Advanced.Attachments.Store("asd", "test", new MemoryStream(new byte[] { 1, 2, 3, 4 })));
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
                await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.Advanced.ClusterTransaction.DeleteCompareExchangeValueAsync("usernames/ayende"));
                await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende"));
            }
        }
    }
}
