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

        [Fact]
        public async Task Incrementing_an_existing_counter_upon_incoming_replication_should_affect_metrics()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                foreach (var store in new [] {storeA, storeB})
                {
                    using (var bulk = store.BulkInsert())
                    {
                        for (int i = 0; i < 1000; i++)
                        {
                            bulk.Store(new User(), "users/" + i);
                        }
                    }

                    for (int i = 0; i < 10; i++)
                    {
                        using (var session = store.OpenSession())
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
                }

                var databaseB = await GetDocumentDatabaseInstanceFor(storeB);

                Assert.True(databaseB.Metrics.Counters.PutsPerSec.Count > 1);
                Assert.True(databaseB.Metrics.Counters.BytesPutsPerSec.Count > 1);

                var oldPutsCount = databaseB.Metrics.Counters.PutsPerSec.Count;
                var oldBytesPutsCount = databaseB.Metrics.Counters.BytesPutsPerSec.Count;

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                databaseB = await GetDocumentDatabaseInstanceFor(storeB);

                Assert.Equal(oldPutsCount * 2, databaseB.Metrics.Counters.PutsPerSec.Count);
                Assert.True(databaseB.Metrics.Counters.BytesPutsPerSec.Count >= oldBytesPutsCount * 2);
            }
        }
    }
}

