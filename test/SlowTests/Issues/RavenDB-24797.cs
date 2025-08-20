using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_24797 : ReplicationTestBase
    {
        public RavenDB_24797(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        public async Task CountersDeleteIncrementConflict_ShouldNotCauseDataCorruption()
        {
            using var storeA = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => "A"
            });
            using var storeB = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => "B"
            });
            using var storeC = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => "C"
            });

            using (var session = storeA.OpenSession())
            {
                session.Store(new User { Name = "CountersOwner" }, TestDocumentId);
                session.SaveChanges();
            }

            var replicationCreationResults = new Dictionary<string, ReplicationCreationResult>
            {
                {"AtoB", CreateReplicationTask(storeA, storeB)},
                {"AtoC", CreateReplicationTask(storeA, storeC)},

                {"BtoA", CreateReplicationTask(storeB, storeA)},
                {"BtoC", CreateReplicationTask(storeB, storeC)},

                {"CtoA", CreateReplicationTask(storeC, storeA)},
                {"CtoB", CreateReplicationTask(storeC, storeB)},
            };

            IncrementTestCounter(storeA);

            EnsureReplication(storeA, storeB, storeC);

            AssertTestCounter(storeA, expectedValue: 1);
            AssertTestCounter(storeB, expectedValue: 1);
            AssertTestCounter(storeC, expectedValue: 1);

            Task.WaitAll(replicationCreationResults.Select(kvp => DisableExternalReplication(kvp.Value)).ToArray());

            IncrementTestCounter(storeA, delta: 1);
            IncrementTestCounter(storeB, delta: 2);

            Task.WaitAll(replicationCreationResults.Select(kvp => EnableExternalReplication(kvp.Value)).ToArray());

            EnsureReplication(storeA, storeB, storeC);

            AssertTestCounter(storeA, expectedValue: 1 + 1 + 2);
            AssertTestCounter(storeB, expectedValue: 1 + 1 + 2);
            AssertTestCounter(storeC, expectedValue: 1 + 1 + 2);

            Task.WaitAll(replicationCreationResults.Select(kvp => DisableExternalReplication(kvp.Value)).ToArray());

            // delete + increment conflict
            IncrementTestCounter(storeA, delta: 1);
            DeleteTestCounter(storeC);

            replicationCreationResults.TryGetValue("CtoA", out var taskCtoA);
            await EnableExternalReplication(taskCtoA);

            replicationCreationResults.TryGetValue("CtoB", out var taskCtoB);
            await EnableExternalReplication(taskCtoB);

            EnsureReplicating(storeC, storeA);
            EnsureReplicating(storeC, storeB);

            replicationCreationResults.TryGetValue("BtoC", out var taskBtoC);
            await EnableExternalReplication(taskBtoC);

            replicationCreationResults.TryGetValue("BtoA", out var taskBtoA);
            await EnableExternalReplication(taskBtoA);

            EnsureReplicating(storeB, storeC);
            EnsureReplicating(storeB, storeA);

            replicationCreationResults.TryGetValue("AtoC", out var taskAtoC);
            await EnableExternalReplication(taskAtoC);

            replicationCreationResults.TryGetValue("AtoB", out var taskAtoB);
            await EnableExternalReplication(taskAtoB);

            EnsureReplicating(storeA, storeC);
            EnsureReplicating(storeA, storeB);

            // counter conflict (delete + blob) should be resolved to blob
            AssertTestCounter(storeA, expectedValue: 5);
            AssertTestCounter(storeB, expectedValue: 5);
            AssertTestCounter(storeC, expectedValue: 5);

            // assert that we didn't end up with corrupted data
            foreach (var store in new[] { storeA, storeB, storeC })
            {
                var db = await GetDocumentDatabaseInstanceFor(store);
                var countersStorage = db.DocumentsStorage.CountersStorage;
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var countersDoc = countersStorage.GetCounterValuesForDocument(ctx, TestDocumentId).FirstOrDefault()?.Values;
                    Assert.NotNull(countersDoc);

                    // should not throw
                    countersDoc.BlittableValidation();
                }
            }
        }

        [RavenFact(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        public async Task CountersDeleteIncrementConflict_ShouldNotCauseDataCorruption2()
        {
            using var storeA = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => "A"
            });
            using var storeB = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => "B"
            });
            using var storeC = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => "C"
            });

            using (var session = storeA.OpenSession())
            {
                session.Store(new User { Name = "CountersOwner" }, TestDocumentId);
                session.SaveChanges();
            }

            var replicationCreationResults = new Dictionary<string, ReplicationCreationResult>
            {
                {"AtoB", CreateReplicationTask(storeA, storeB)},
                {"AtoC", CreateReplicationTask(storeA, storeC)},

                {"BtoA", CreateReplicationTask(storeB, storeA)},
                {"BtoC", CreateReplicationTask(storeB, storeC)},

                {"CtoA", CreateReplicationTask(storeC, storeA)},
                {"CtoB", CreateReplicationTask(storeC, storeB)},
            };

            IncrementTestCounter(storeA);

            EnsureReplication(storeA, storeB, storeC);

            AssertTestCounter(storeA, expectedValue: 1);
            AssertTestCounter(storeB, expectedValue: 1);
            AssertTestCounter(storeC, expectedValue: 1);

            Task.WaitAll(replicationCreationResults.Select(kvp => DisableExternalReplication(kvp.Value)).ToArray());

            IncrementTestCounter(storeA, delta: 1);
            IncrementTestCounter(storeB, delta: 2);

            Task.WaitAll(replicationCreationResults.Select(kvp => EnableExternalReplication(kvp.Value)).ToArray());

            //await Task.Delay(1000);
            EnsureReplication(storeA, storeB, storeC);

            AssertTestCounter(storeA, expectedValue: 1 + 1 + 2);
            AssertTestCounter(storeB, expectedValue: 1 + 1 + 2);
            AssertTestCounter(storeC, expectedValue: 1 + 1 + 2);

            Task.WaitAll(replicationCreationResults.Select(kvp => DisableExternalReplication(kvp.Value)).ToArray());

            // delete + increment conflict
            IncrementTestCounter(storeC, delta: 1);
            DeleteTestCounter(storeA);

            replicationCreationResults.TryGetValue("CtoA", out var taskCtoA);
            await EnableExternalReplication(taskCtoA);

            replicationCreationResults.TryGetValue("CtoB", out var taskCtoB);
            await EnableExternalReplication(taskCtoB);

            EnsureReplicating(storeC, storeA);
            EnsureReplicating(storeC, storeB);

            replicationCreationResults.TryGetValue("BtoC", out var taskBtoC);
            await EnableExternalReplication(taskBtoC);

            replicationCreationResults.TryGetValue("BtoA", out var taskBtoA);
            await EnableExternalReplication(taskBtoA);

            EnsureReplicating(storeB, storeC);
            EnsureReplicating(storeB, storeA);

            replicationCreationResults.TryGetValue("AtoC", out var taskAtoC);
            await EnableExternalReplication(taskAtoC);

            replicationCreationResults.TryGetValue("AtoB", out var taskAtoB);
            await EnableExternalReplication(taskAtoB);

            EnsureReplicating(storeA, storeC);
            EnsureReplicating(storeA, storeB);

            // counter conflict (delete + blob) should be resolved to blob
            AssertTestCounter(storeA, expectedValue: 5);
            AssertTestCounter(storeB, expectedValue: 5);
            AssertTestCounter(storeC, expectedValue: 5);
        }

        [RavenFact(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        public void CountersDeleteIncrementConflict_ShouldBeResolvedToBlob_WithNoErrors()
        {
            using var storeA = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => "A"
            });
            using var storeB = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => "B"
            });

            using (var session = storeA.OpenSession())
            {
                session.Store(new User { Name = "CountersOwner" }, TestDocumentId);
                session.SaveChanges();
            }

            var replicationCreationResults = new Dictionary<string, ReplicationCreationResult>
            {
                {"AtoB", CreateReplicationTask(storeA, storeB)},
                {"BtoA", CreateReplicationTask(storeB, storeA)},
            };

            IncrementTestCounter(storeA);

            EnsureReplicating(storeA, storeB);
            EnsureReplicating(storeB, storeA);

            AssertTestCounter(storeA, expectedValue: 1);
            AssertTestCounter(storeB, expectedValue: 1);
            
            Task.WaitAll(replicationCreationResults.Select(kvp => DisableExternalReplication(kvp.Value)).ToArray());
            
            // delete + increment conflict
            DeleteTestCounter(storeA);
            IncrementTestCounter(storeB, delta: 2);

            Task.WaitAll(replicationCreationResults.Select(kvp => EnableExternalReplication(kvp.Value)).ToArray());

            // should be resolved to blob without replication getting stuck (due to IndexOutOfRange)
            EnsureReplicating(storeA, storeB);
            EnsureReplicating(storeB, storeA);

            AssertTestCounter(storeA, 3);
            AssertTestCounter(storeB, 3);
        }

        [RavenFact(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        public void CountersDeleteIncrementConflict_ShouldBeResolvedToBlob_AndHAveCounterNameInMetadata()
        {
            using var storeA = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => "A"
            });
            using var storeB = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => "B"
            });

            using (var session = storeA.OpenSession())
            {
                session.Store(new User { Name = "CountersOwner" }, TestDocumentId);
                session.SaveChanges();
            }

            var replicationCreationResults = new Dictionary<string, ReplicationCreationResult>
            {
                {"AtoB", CreateReplicationTask(storeA, storeB)},
                {"BtoA", CreateReplicationTask(storeB, storeA)},
            };

            IncrementTestCounter(storeA);

            EnsureReplicating(storeA, storeB);
            EnsureReplicating(storeB, storeA);

            AssertTestCounter(storeA, expectedValue: 1);
            AssertTestCounter(storeB, expectedValue: 1);
            
            Task.WaitAll(replicationCreationResults.Select(kvp => DisableExternalReplication(kvp.Value)).ToArray());

            // delete + increment conflict
            DeleteTestCounter(storeA);
            IncrementTestCounter(storeB, delta: 2);

            Task.WaitAll(replicationCreationResults.Select(kvp => EnableExternalReplication(kvp.Value)).ToArray());

            EnsureReplicating(storeA, storeB);
            EnsureReplicating(storeB, storeA);

            // document should have the counter name in metadata
            using (var session = storeA.OpenSession())
            {
                var doc = session.Load<User>(TestDocumentId);
                var metadataCounters = session.Advanced.GetCountersFor(doc);

                Assert.NotNull(metadataCounters);
                Assert.Contains(TestCounterName, metadataCounters);
            }
            using (var session = storeB.OpenSession())
            {
                var doc = session.Load<User>(TestDocumentId);
                var metadataCounters = session.Advanced.GetCountersFor(doc);

                Assert.NotNull(metadataCounters);
                Assert.Contains(TestCounterName, metadataCounters);
            }
        }

        private const string TestCounterName = "someCounter";
        private const string TestDocumentId = "users/1-A";
        private void EnsureReplication(DocumentStore storeA, DocumentStore storeB, DocumentStore storeC)
        {
            EnsureReplicating(storeA, storeB);
            EnsureReplicating(storeA, storeC);

            EnsureReplicating(storeB, storeA);
            EnsureReplicating(storeB, storeC);

            EnsureReplicating(storeC, storeA);
            EnsureReplicating(storeC, storeB);
        }
        private static void IncrementTestCounter(DocumentStore store, long delta = 1)
        {
            using (var session = store.OpenSession())
            {
                session.CountersFor(TestDocumentId).Increment(TestCounterName, delta);
                session.SaveChanges();
            }
        }
        private static void DeleteTestCounter(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.CountersFor(TestDocumentId).Delete(TestCounterName);
                session.SaveChanges();
            }
        }
        private static void AssertTestCounter(DocumentStore store, long expectedValue)
        {
            using (var session = store.OpenSession())
            {
                var counter = session.CountersFor(TestDocumentId).Get(TestCounterName);
                Assert.NotNull(counter);
                Assert.Equal(expectedValue, counter.Value);
            }
        }
        private static async Task DisableExternalReplication(ReplicationCreationResult replicationCreationResult)
        {
            replicationCreationResult.Configuration.Disabled = true;
            await SendUpdateExternalReplicationConfiguration(replicationCreationResult);
        }
        private static async Task EnableExternalReplication(ReplicationCreationResult taskAtoB)
        {
            taskAtoB.Configuration.Disabled = false;
            await SendUpdateExternalReplicationConfiguration(taskAtoB);
        }
        private static async Task SendUpdateExternalReplicationConfiguration(ReplicationCreationResult replicationCreationResult)
        {
            var op = new UpdateExternalReplicationOperation(replicationCreationResult.Configuration);
            var result = await replicationCreationResult.OwnerStore.Maintenance.SendAsync(op);
            Assert.NotNull(result.RaftCommandIndex);
        }
        private static ReplicationCreationResult CreateReplicationTask(DocumentStore sourceStore, DocumentStore destStore)
        {
            var watcher = new ExternalReplication(destStore.Database, $"ConnectionString{sourceStore.Database}to{destStore.Database}") { Name = $"{sourceStore.Database} to {destStore.Database}" };

            var result = sourceStore.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = watcher.ConnectionStringName,
                Database = watcher.Database,
                TopologyDiscoveryUrls = destStore.Urls
            }));
            Assert.NotNull(result.RaftCommandIndex);

            var replicationOperation = new UpdateExternalReplicationOperation(watcher);
            var replicationOperationResult = sourceStore.Maintenance.Send(replicationOperation);
            Assert.NotNull(replicationOperationResult.RaftCommandIndex);

            watcher.TaskId = replicationOperationResult.TaskId;
            return new ReplicationCreationResult { Configuration = watcher, OwnerStore = sourceStore };
        }
        private struct ReplicationCreationResult
        {
            public ExternalReplication Configuration;
            public DocumentStore OwnerStore;
        }
    }
}
