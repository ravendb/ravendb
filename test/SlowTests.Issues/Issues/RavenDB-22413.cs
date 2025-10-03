using System;
using System.Linq;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22413 : ReplicationTestBase
    {
        public RavenDB_22413(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicationMajority(Options options)
        {
            var (_, leader) = await CreateRaftCluster(3);

            options.Server = leader;
            options.ReplicationFactor = 3;

            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), majority: true);
                    session.SaveChanges();
                }

                // fail the node to to where the data is sent
                await DisposeServerAndWaitForFinishOfDisposalAsync(Servers.First());

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel2"
                    }, "users/2");
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(7), majority: true);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var u = session.Load<User>("users/2");
                    Assert.NotNull(u);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
