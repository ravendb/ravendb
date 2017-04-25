using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents.Replication;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_6173 : ReplicationTestsBase
    {
        public class Foo
        {            
        }

        public class Bar
        {
        }

        [Fact(Skip = "wait for implementing etl over raft")]
        public async Task Topology_should_fetch_ETL_destinations_properly()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            using (var storeC = GetDocumentStore())
            using (var storeD = GetDocumentStore())
            {
                var dbA = await GetDocumentDatabaseInstanceFor(storeA);
                var dbB = await GetDocumentDatabaseInstanceFor(storeB);
                var dbC = await GetDocumentDatabaseInstanceFor(storeC);
                var dbD = await GetDocumentDatabaseInstanceFor(storeD);

                using (var session = storeA.OpenSession())
                {
                    session.Store(new Foo(),"foo");
                    session.Store(new Bar(),"bar");
                    session.SaveChanges();    
                }

                var etl = new Dictionary<string,string>
                {
                    { "bars", "this.Foo = 'bar'" } //dummy script
                };

                /*SetupReplication(storeA, etl, storeB);

                using (var session = storeA.OpenSession())
                {
                    var replicationDocument =
                        session.Load<ReplicationDocument>(Constants.Documents.Replication.ReplicationConfigurationDocument);

                    //add non-etl destination
                    replicationDocument.Destinations.Add(new ReplicationNode
                    {
                        Database = storeD.DefaultDatabase,
                        Url = storeD.Url
                    });

                    session.SaveChanges();
                }

                SetupReplication(storeB, etl, storeC);*/

                WaitForDocumentToReplicate<Bar>(storeC, "bar", 10000);

                var topologyInfo = GetFullTopology(storeA);
                
                Assert.NotNull(topologyInfo); //sanity check
                var idOfA = dbA.DbId.ToString();
                var idOfB = dbB.DbId.ToString();
                var idOfC = dbC.DbId.ToString();
                var idOfD = dbD.DbId.ToString();

                var nodesOfA = topologyInfo.NodesById[idOfA];
                var nodesOfB = topologyInfo.NodesById[idOfB];
                var nodesOfC = topologyInfo.NodesById[idOfC];

                Assert.Equal(2, nodesOfA.Outgoing.Count);
                Assert.Equal(idOfB, nodesOfA.Outgoing.FirstOrDefault(x => x.DbId == idOfB)?.DbId);
                Assert.Equal(true, nodesOfA.Outgoing.FirstOrDefault(x => x.DbId == idOfB)?.IsETLNode);

                Assert.Equal(idOfD, nodesOfA.Outgoing.FirstOrDefault(x => x.DbId == idOfD)?.DbId);
                Assert.Equal(false, nodesOfA.Outgoing.FirstOrDefault(x => x.DbId == idOfD).IsETLNode);

                Assert.Equal(1, nodesOfB.Outgoing.Count);
                Assert.Equal(idOfC, nodesOfB.Outgoing.FirstOrDefault(x => x.DbId == idOfC)?.DbId);
                Assert.Equal(true, nodesOfB.Outgoing.FirstOrDefault(x => x.DbId == idOfC)?.IsETLNode);

                Assert.Equal(0, nodesOfC.Outgoing.Count);
            }
        }
    }
}
