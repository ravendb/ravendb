using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12132 : ReplicationTestBase
    {
        public RavenDB_12132(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanPutObjectWithId()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", new User
            {
                Id = "users/1",
                Name = "Grisha"
            }, 0));

            Assert.True(res.Successful);
            Assert.Equal("Grisha", res.Value.Name);
            Assert.Equal("users/1", res.Value.Id);
        }

        [Fact]
        public async Task CanCreateClusterTransactionRequest1()
        {
            var (_, leader) = await CreateRaftCluster(3);
            using (var leaderStore = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3
            }))
            {
                var user = new User
                {
                    Id = "this/is/my/id",
                    Name = "Grisha"
                };
                using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user);
                    await session.SaveChangesAsync();

                    var userFromCluster = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende")).Value;
                    Assert.Equal(user.Name, userFromCluster.Name);
                    Assert.Equal(user.Id, userFromCluster.Id);
                }
            }
        }
    }
}
