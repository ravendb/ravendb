using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Graph;
using FastTests.Server.Replication;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13767 : ReplicationTestBase
    {
        public RavenDB_13767(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task PatchingClusterTransactionDocumentShouldWork()
        {
            var (_, leader) = await CreateRaftCluster(3);
            using (var leaderStore = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3
            }))
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
