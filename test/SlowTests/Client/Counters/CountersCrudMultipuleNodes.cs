using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Http;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client.Counters
{
    public class CountersCrudMultipuleNodes : ClusterTestBase
    {
        private const string DocId = "users/1-A";

        [Fact]
        public async Task IncrementCounter()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var db = await CreateDatabaseInCluster("MainDB", 3, leader.WebUrl);

            var serverNodes = db.Servers.Select(s => new ServerNode
            {
                ClusterTag = s.ServerStore.NodeTag,
                Database = "MainDB",
                Url = s.WebUrl
            }).ToList();

            var conventions = new DocumentConventions
            {
                DisableTopologyUpdates = true
            };

            using (var leaderStore = new DocumentStore
            {
                Database = "MainDB",
                Urls = new[] { leader.WebUrl },
                Conventions = conventions
            }.Initialize())        
            using (var session = leaderStore.OpenSession())
            {
                session.Store(new User { Name = "Aviv" });
                session.SaveChanges();
            }            

            foreach (var node in serverNodes)
            {
                using (var store = new DocumentStore
                {
                    Database = node.Database,
                    Urls = new[] { node.Url },
                    Conventions = conventions
                }.Initialize())
                {
                    store.Operations.Send(new IncrementCounterOperation(DocId, "likes", 10));
                }
            }

            using (var leaderStore = new DocumentStore
            {
                Database = "MainDB",
                Urls = new[] { leader.WebUrl },
                Conventions = conventions
            }.Initialize())
            {
                var val = leaderStore.Operations.Send(new GetCounterValueOperation(DocId, "likes"));
                Assert.Equal(30, val);
            }

        }
    }
}
