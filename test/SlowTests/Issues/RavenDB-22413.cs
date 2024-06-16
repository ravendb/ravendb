using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Replication;
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

        [RavenFact(RavenTestCategory.Replication)]
        public async Task ReplicationMajority()
        {
            var (_, leader) = await CreateRaftCluster(3);

            using (var store = GetDocumentStore(new Options
                   {
                       Server = leader,
                       ReplicationFactor = 3
                   }))
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
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), majority: true);
                    session.SaveChanges();
                }

                // WaitForUserToContinueTheTest(store, false);

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
