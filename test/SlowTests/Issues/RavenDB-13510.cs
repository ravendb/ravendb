using System.Threading.Tasks;
using FastTests.Server.Replication;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13510 : ReplicationTestBase
    {
        [Fact]
        public async Task Writing_of_a_new_counter_group_document_upon_incoming_replication_should_affect_metrics()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var bulk = storeA.BulkInsert())
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        bulk.Store(new User(), "users/" + i);
                    }
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = storeA.OpenSession())
                    {
                        for (int j = 0; j < 100; j++)
                        {
                            var countersFor = session.CountersFor("users/" + (i * 100 + j));
                            countersFor.Increment("myCounter1");
                            countersFor.Increment("myCounter2");
                            countersFor.Increment("myCounter3");
                        }

                        session.SaveChanges();
                    }
                }

                var databaseA = await GetDocumentDatabaseInstanceFor(storeA);

                Assert.True(databaseA.Metrics.Counters.PutsPerSec.Count > 1);
                Assert.True(databaseA.Metrics.Counters.BytesPutsPerSec.Count > 1);

                await SetupReplicationAsync(storeA, storeB);

                EnsureReplicating(storeA, storeB);

                var databaseB = await GetDocumentDatabaseInstanceFor(storeB);

                Assert.True(databaseB.Metrics.Counters.PutsPerSec.Count > 1);
                Assert.True(databaseB.Metrics.Counters.BytesPutsPerSec.Count > 1);

            }
        }
    }
}

