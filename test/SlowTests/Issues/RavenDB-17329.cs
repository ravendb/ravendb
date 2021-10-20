using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17329 : ClusterTestBase
    {
        public RavenDB_17329(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task DeletingCounterWhenFirstPartialValueIsEmptyAndRemote_ShouldNotGenerateCorruptedDeleteCv()
        {
            var (_, leader) = await CreateRaftCluster(2);
            var dbName = GetDatabaseName();
            var db = await CreateDatabaseInCluster(dbName, 2, leader.WebUrl);

            var stores = db.Servers
                .Select(s => new DocumentStore
                {
                    Database = dbName, 
                    Urls = new[] {s.WebUrl}, 
                    Conventions = new DocumentConventions
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                .ToList();
            try
            {
                using (var s = stores[0].OpenSession())
                {
                    s.Advanced.WaitForReplicationAfterSaveChanges();
                    s.Store(new User { Name = "Aviv" }, "users/1");
                    s.CountersFor("users/1").Increment("likes", 30);
                    s.SaveChanges();
                }

                using (var s = stores[1].OpenSession())
                {
                    s.Advanced.WaitForReplicationAfterSaveChanges();
                    s.CountersFor("users/1").Increment("dislikes", 2);
                    s.SaveChanges();
                }

                using (var s = stores[1].OpenSession())
                {
                    s.Advanced.WaitForReplicationAfterSaveChanges();
                    s.CountersFor("users/1").Delete("dislikes");
                    s.SaveChanges();
                }

                foreach (var store in stores)
                {
                    var countersDetail = store.Operations.Send(new GetCountersOperation("users/1", new[] { "dislikes" }));
                    Assert.Equal(1, countersDetail.Counters.Count);
                    Assert.Null(countersDetail.Counters[0]);
                }

                foreach (var server in db.Servers)
                {
                    await EnsureNoReplicationLoop(server, dbName);
                }

                var rec = stores[0].Maintenance.Server.Send(new GetDatabaseRecordOperation(dbName));
                var topology = rec.Topology;
                Assert.Equal(2, topology.Count);
                Assert.Equal(2, topology.Members.Count);

                using (var s = stores[0].OpenSession())
                {
                    s.Store(new User { Name = "John doe" }, "users/2");
                    s.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(stores[1], "users/2", x => x.Name == "John doe"));

                using (var s = stores[1].OpenSession())
                {
                    s.Store(new User { Name = "Jane doe" }, "users/3");
                    s.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(stores[0], "users/3", x => x.Name == "Jane doe"));
            }
            finally
            {
                foreach (var item in stores)
                {
                    item.Dispose();
                }
            }
        }
    }
}
