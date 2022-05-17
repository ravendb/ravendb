using Tests.Infrastructure;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Session;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13767 : ReplicationTestBase
    {
        public RavenDB_13767(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task PatchingClusterTransactionDocumentShouldWork(Options options)
        {
            var (_, leader) = await CreateRaftCluster(3);
            options.Server = leader;
            options.ReplicationFactor = 3;

            using (var leaderStore = GetDocumentStore(options))
            {
                var user1 = new User
                {
                    Name = "Patch op boy"
                };

                using (var session = leaderStore.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(user1, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = leaderStore.OpenSession())
                {
                    session.Advanced.Patch<User, string>("foo/bar", x => x.Name, "Pet shop boys");
                    session.SaveChanges();
                }
            }
        }
    }
}
