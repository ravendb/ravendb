using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;
using static Raven.Server.Documents.CountersStorage;

namespace SlowTests.Issues
{
    public class RavenDB_22835 : ClusterTestBase
    {
        public RavenDB_22835(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Counters)]
        public async Task SplittingCountersManyTimes_ShouldNotCauseErrorsDuringImport()
        {
            var rand = new Random(12345);
            const string id = "users/1";

            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new User(), id);
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var countersFor = session.CountersFor(id);

                for (int j = 0; j < 100; j++)
                {
                    var strSize = (int)rand.NextInt64(5, 15);
                    var s = GenRandomString(rand, strSize);

                    countersFor.Increment(s, j);
                }

                session.SaveChanges();
            }

            // starting import 1
            var temp1 = GetTempFileName();
            var export = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), temp1);
            await export.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

            using var store2 = GetDocumentStore();
            var import = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), temp1);
            await import.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

            // import 1 went well, now adding more counters to this doc
            AddMoreCountersForDoc(store2, rand, id);

            // starting import 2
            var temp2 = GetTempFileName();
            export = await store2.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), temp2);
            await export.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

            using var store3 = GetDocumentStore();
            import = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), temp2);
            await import.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

            // import 2 went well, now adding more counters to this doc
            AddMoreCountersForDoc(store3, rand, id);

            // all good - no exception was thrown during imports or when adding new counters
        }

        [RavenFact(RavenTestCategory.Counters)]
        public async Task FixCorruptedCounterData()
        {
            var rand = new Random(12345);
            const string id = "users/1";

            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new User(), id);
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var countersFor = session.CountersFor(id);

                for (int j = 0; j < 100; j++)
                {
                    var strSize = (int)rand.NextInt64(5, 15);
                    var s = GenRandomString(rand, strSize);

                    countersFor.Increment(s, j);
                }

                session.SaveChanges();
            }

            var temp1 = GetTempFileName();
            var export = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), temp1);
            await export.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

            using var store2 = GetDocumentStore();
            var import = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), temp1);
            await import.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

            var db = await GetDatabase(store.Database);

            // deliberately corrupt counters group data
            CorruptCountersData(db);
            
            AddMoreCountersForDoc(store2, rand, id);

            // verify that counters data is corrupted - we're missing a counter name
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var counterGroup = (CounterReplicationItem)db.DocumentsStorage.CountersStorage.GetCountersFrom(context, etag: 0).First();
                Assert.True(counterGroup.Values.TryGet(Values, out BlittableJsonReaderObject counterValues));
                Assert.True(counterGroup.Values.TryGet(CounterNames, out BlittableJsonReaderObject counterNames));

                Assert.NotEqual(counterValues.Count, counterNames.Count);

                BlittableJsonReaderObject.PropertyDetails p = default;
                counterValues.GetPropertyByIndex(0, ref p);
                var firstCounter = p.Name;

                Assert.False(counterNames.TryGet(firstCounter, out string _));
            }

            // call FixCounters tool
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var numOfFixes = db.CountersRepairTask.FixCountersForDocument(context, id);
                Assert.Equal(1, numOfFixes);

                tx.Commit();
            }

            // calling FixCounters tool again should return 0 - no CounterGroup was fixed
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenWriteTransaction())
            {
                var numOfFixes = db.CountersRepairTask.FixCountersForDocument(context, id);
                Assert.Equal(0, numOfFixes);
            }

            // assert that counters data is fixed
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var counterGroups = db.DocumentsStorage.CountersStorage.GetCountersFrom(context, etag: 0);

                foreach (var item in counterGroups)
                {
                    var counterGroup = item as CounterReplicationItem;
                    Assert.NotNull(counterGroup);

                    Assert.True(counterGroup.Values.TryGet(Values, out BlittableJsonReaderObject counterValues));
                    Assert.True(counterGroup.Values.TryGet(CounterNames, out BlittableJsonReaderObject counterNames));

                    Assert.Equal(counterValues.Count, counterNames.Count);

                    BlittableJsonReaderObject.PropertyDetails p1 = default, p2 = default;
                    for (int i = 0; i < counterValues.Count; i++)
                    {
                        counterValues.GetPropertyByIndex(i, ref p1);
                        counterNames.GetPropertyByIndex(i, ref p2);

                        Assert.Equal(p1.Name, p2.Name);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Counters)]
        public async Task FixCorruptedCounterData_InCluster()
        {
            var rand = new Random(12345);
            const string id = "users/1";

            var cluster = await CreateRaftCluster(numberOfNodes: 3, watcherCluster: true);
            var dbName = GetDatabaseName();
            await CreateDatabaseInCluster(dbName, replicationFactor: 3, cluster.Leader.WebUrl);

            using var leaderStore = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = cluster.Leader,
                ModifyDatabaseName = _ => dbName,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            });

            using (var session = leaderStore.OpenSession())
            {
                session.Store(new User(), id);
                session.SaveChanges();
            }

            using (var session = leaderStore.OpenSession())
            {
                var countersFor = session.CountersFor(id);

                for (int j = 0; j < 100; j++)
                {
                    var strSize = (int)rand.NextInt64(5, 15);
                    var s = GenRandomString(rand, strSize);

                    countersFor.Increment(s, j);
                }

                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                session.SaveChanges();
            }

            var nodeB = cluster.Nodes.First(x => x != cluster.Leader);

            using var storeB = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = nodeB,
                ModifyDatabaseName = _ => dbName,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            });

            AddMoreCountersForDoc(storeB, rand, id, numOfIterations: 10, replicas: 2);

            var nodeC = cluster.Nodes.Single(x => x != cluster.Leader && x != nodeB);

            using var storeC = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = nodeC,
                ModifyDatabaseName = _ => dbName,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            });

            AddMoreCountersForDoc(storeC, rand, id, numOfIterations: 10, replicas: 2);

            foreach (var node in cluster.Nodes)
            {
                var db = await GetDatabase(node, dbName);

                // deliberately corrupt counters group data on each node
                CorruptCountersData(db);

                // verify that counters data is corrupted - we're missing a counter name
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var counterGroup = (CounterReplicationItem)db.DocumentsStorage.CountersStorage.GetCountersFrom(context, etag: 0).First();
                    Assert.True(counterGroup.Values.TryGet(Values, out BlittableJsonReaderObject counterValues));
                    Assert.True(counterGroup.Values.TryGet(CounterNames, out BlittableJsonReaderObject counterNames));

                    Assert.NotEqual(counterValues.Count, counterNames.Count);
                    BlittableJsonReaderObject.PropertyDetails p = default;
                    counterValues.GetPropertyByIndex(0, ref p);
                    var firstCounter = p.Name;

                    Assert.False(counterNames.TryGet(firstCounter, out string _));
                }
            }

            foreach (var node in cluster.Nodes)
            {                
                var db = await GetDatabase(node, dbName);

                // call FixCounters tool
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var numOfFixes = db.CountersRepairTask.FixCountersForDocument(context, id);
                    Assert.Equal(1, numOfFixes);

                    tx.Commit();
                }

                // assert that counters data is fixed

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var counterGroups = db.DocumentsStorage.CountersStorage.GetCountersFrom(context, etag: 0);

                    foreach (var item in counterGroups)
                    {
                        var counterGroup = item as CounterReplicationItem;
                        Assert.NotNull(counterGroup);

                        Assert.True(counterGroup.Values.TryGet(Values, out BlittableJsonReaderObject counterValues));
                        Assert.True(counterGroup.Values.TryGet(CounterNames, out BlittableJsonReaderObject counterNames));

                        Assert.Equal(counterValues.Count, counterNames.Count);

                        BlittableJsonReaderObject.PropertyDetails p1 = default, p2 = default;
                        for (int i = 0; i < counterValues.Count; i++)
                        {
                            counterValues.GetPropertyByIndex(i, ref p1);
                            counterNames.GetPropertyByIndex(i, ref p2);

                            Assert.Equal(p1.Name, p2.Name);
                        }
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Counters)]
        public async Task FixCorruptedCounterData_MultipleDocs()
        {
            var rand = new Random(12345);
            List<string> docIdsToCorrupt = ["users/1", "users/10", "users/20", "users/100"];

            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                for (int i = 1; i <= 100; i++)
                {
                    session.Store(new User(), "users/" + i);
                }

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                for (int i = 1; i <= 100; i++)
                {
                    var countersFor = session.CountersFor("users/" + i);

                    for (int j = 0; j < 100; j++)
                    {
                        var strSize = (int)rand.NextInt64(5, 15);
                        var s = GenRandomString(rand, strSize);

                        countersFor.Increment(s, j);
                    }
                }

                session.SaveChanges();
            }

            var db = await GetDatabase(store.Database);

            foreach (var id in docIdsToCorrupt)
            {
                // deliberately corrupt counters group data
                CorruptCountersData(db, id);

                // verify that counters data is corrupted - we're missing a counter name
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var readTable = new Table(CountersSchema, context.Transaction.InnerTransaction);
                    TableValueReader tvr;
                    using (DocumentIdWorker.GetSliceFromId(context, id, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                    {
                        Assert.True(readTable.SeekOnePrimaryKeyPrefix(documentKeyPrefix, out tvr));
                    }

                    var data = GetCounterValuesData(context, ref tvr);
                    Assert.True(data.TryGet(Values, out BlittableJsonReaderObject counterValues));
                    Assert.True(data.TryGet(CounterNames, out BlittableJsonReaderObject counterNames));

                    Assert.NotEqual(counterValues.Count, counterNames.Count);
                    BlittableJsonReaderObject.PropertyDetails p = default;
                    counterValues.GetPropertyByIndex(0, ref p);
                    var firstCounter = p.Name;

                    Assert.False(counterNames.TryGet(firstCounter, out string _));
                }
            }

            // call FixCounters tool
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var numOfFixes = db.CountersRepairTask.FixCountersForDocuments(context, docIdsToCorrupt, hasMore: false);
                Assert.Equal(4, numOfFixes);

                tx.Commit();
            }

            // assert that counters data was fixed in all corrupted documents
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var counterGroups = db.DocumentsStorage.CountersStorage.GetCountersFrom(context, etag: 0);

                foreach (var item in counterGroups)
                {
                    var counterGroup = item as CounterReplicationItem;
                    Assert.NotNull(counterGroup);

                    Assert.True(counterGroup.Values.TryGet(Values, out BlittableJsonReaderObject counterValues));
                    Assert.True(counterGroup.Values.TryGet(CounterNames, out BlittableJsonReaderObject counterNames));

                    Assert.Equal(counterValues.Count, counterNames.Count);

                    BlittableJsonReaderObject.PropertyDetails p1 = default, p2 = default;
                    for (int i = 0; i < counterValues.Count; i++)
                    {
                        counterValues.GetPropertyByIndex(i, ref p1);
                        counterNames.GetPropertyByIndex(i, ref p2);

                        Assert.Equal(p1.Name, p2.Name);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Counters)]
        public async Task ShouldFixCorruptedCountersDataOnImport()
        {
            var rand = new Random(12345);
            List<string> docIdsToCorrupt = ["users/1", "users/10", "users/20", "users/100"];

            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                for (int i = 1; i <= 100; i++)
                {
                    session.Store(new User(), "users/" + i);
                }

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                for (int i = 1; i <= 100; i++)
                {
                    var countersFor = session.CountersFor("users/" + i);

                    for (int j = 0; j < 100; j++)
                    {
                        var strSize = (int)rand.NextInt64(5, 15);
                        var s = GenRandomString(rand, strSize);

                        countersFor.Increment(s, j);
                    }
                }

                session.SaveChanges();
            }

            var db = await GetDatabase(store.Database);

            foreach (var id in docIdsToCorrupt)
            {
                // deliberately corrupt counters group data
                CorruptCountersData(db, id);

                // verify that counters data is corrupted - we're missing a counter name
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var readTable = new Table(CountersSchema, context.Transaction.InnerTransaction);
                    TableValueReader tvr;
                    using (DocumentIdWorker.GetSliceFromId(context, id, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                    {
                        Assert.True(readTable.SeekOnePrimaryKeyPrefix(documentKeyPrefix, out tvr));
                    }

                    var data = GetCounterValuesData(context, ref tvr);
                    Assert.True(data.TryGet(Values, out BlittableJsonReaderObject counterValues));
                    Assert.True(data.TryGet(CounterNames, out BlittableJsonReaderObject counterNames));

                    Assert.NotEqual(counterValues.Count, counterNames.Count);
                    BlittableJsonReaderObject.PropertyDetails p = default;
                    counterValues.GetPropertyByIndex(0, ref p);
                    var firstCounter = p.Name;

                    Assert.False(counterNames.TryGet(firstCounter, out string _));
                }
            }

            // backup the database with bad counters

            var backupPath = NewDataPath(suffix: "BackupFolder");

            var config = Backup.CreateBackupConfiguration(backupPath);
            var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
            var operation = new GetPeriodicBackupStatusOperation(backupTaskId);

            var backupStatus = store.Maintenance.Send(operation);
            var backupOperationId = backupStatus.Status.LastOperationId;

            var backupOperation = store.Maintenance.Send(new GetOperationStateOperation(backupOperationId.Value));
            Assert.Equal(OperationStatus.Completed, backupOperation.Status);

            // restore the database with a different name
            var restoredDatabaseName = $"restored_database-{Guid.NewGuid()}";
            var backupLocation = Directory.GetDirectories(backupPath).First();

            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                BackupLocation = backupLocation,
                DatabaseName = restoredDatabaseName
            }))
            {
                var restoredDb = await GetDatabase(restoredDatabaseName);

                // wait for FixCounters to complete
                Assert.True(await WaitForValueAsync(() =>
                {
                    using (restoredDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        return DocumentsStorage.ReadLastFixedCounterKey(context.Transaction.InnerTransaction) == CountersRepairTask.Completed;
                    }
                }, expectedVal: true, timeout: 10_000));

                // assert that counters data was fixed in all corrupted documents
                using (restoredDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var counterGroups = restoredDb.DocumentsStorage.CountersStorage.GetCountersFrom(context, etag: 0);

                    foreach (var item in counterGroups)
                    {
                        var counterGroup = item as CounterReplicationItem;
                        Assert.NotNull(counterGroup);

                        Assert.True(counterGroup.Values.TryGet(Values, out BlittableJsonReaderObject counterValues));
                        Assert.True(counterGroup.Values.TryGet(CounterNames, out BlittableJsonReaderObject counterNames));

                        Assert.Equal(counterValues.Count, counterNames.Count);

                        BlittableJsonReaderObject.PropertyDetails p1 = default, p2 = default;
                        for (int i = 0; i < counterValues.Count; i++)
                        {
                            counterValues.GetPropertyByIndex(i, ref p1);
                            counterNames.GetPropertyByIndex(i, ref p2);

                            Assert.Equal(p1.Name, p2.Name);
                        }
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Counters)]
        public async Task AfterDatabaseRestartFixCountersToolShouldBeLaunched()
        {
            var rand = new Random(12345);
            List<string> docIdsToCorrupt = ["users/1", "users/10", "users/20", "users/100"];

            using var store = GetDocumentStore(new Options
            {
                RunInMemory = false
            });

            using (var session = store.OpenSession())
            {
                for (int i = 1; i <= 100; i++)
                {
                    session.Store(new User(), "users/" + i);
                }

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                for (int i = 1; i <= 100; i++)
                {
                    var countersFor = session.CountersFor("users/" + i);

                    for (int j = 0; j < 100; j++)
                    {
                        var strSize = (int)rand.NextInt64(5, 15);
                        var s = GenRandomString(rand, strSize);

                        countersFor.Increment(s, j);
                    }
                }

                session.SaveChanges();
            }

            var db = await GetDatabase(store.Database);

            foreach (var id in docIdsToCorrupt)
            {
                // deliberately corrupt counters group data
                CorruptCountersData(db, id);

                // verify that counters data is corrupted - we're missing a counter name
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var readTable = new Table(CountersSchema, context.Transaction.InnerTransaction);
                    TableValueReader tvr;
                    using (DocumentIdWorker.GetSliceFromId(context, id, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                    {
                        Assert.True(readTable.SeekOnePrimaryKeyPrefix(documentKeyPrefix, out tvr));
                    }

                    var data = GetCounterValuesData(context, ref tvr);
                    Assert.True(data.TryGet(Values, out BlittableJsonReaderObject counterValues));
                    Assert.True(data.TryGet(CounterNames, out BlittableJsonReaderObject counterNames));

                    Assert.NotEqual(counterValues.Count, counterNames.Count);
                    BlittableJsonReaderObject.PropertyDetails p = default;
                    counterValues.GetPropertyByIndex(0, ref p);
                    var firstCounter = p.Name;

                    Assert.False(counterNames.TryGet(firstCounter, out string _));
                }
            }

            // override LastCounterFixed in order to launch the FixCounters tool upon restart
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                db.DocumentsStorage.SetLastFixedCounterKey(context, lastKey: "users/0");
                tx.Commit();
            }

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var lastCounterFixed = DocumentsStorage.ReadLastFixedCounterKey(context.Transaction.InnerTransaction);
                Assert.Equal("users/0", lastCounterFixed);
            }

            // reload database 
            await ReloadDatabase(store);
            db = await GetDatabase(store.Database);

            // wait for FixCounters to complete
            Assert.True(await WaitForValueAsync(() =>
            {
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    return DocumentsStorage.ReadLastFixedCounterKey(context.Transaction.InnerTransaction) == CountersRepairTask.Completed;
                }
            }, expectedVal: true, timeout: 10_000));

            // assert that counters data was fixed in all corrupted documents
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var counterGroups = db.DocumentsStorage.CountersStorage.GetCountersFrom(context, etag: 0);

                foreach (var item in counterGroups)
                {
                    var counterGroup = item as CounterReplicationItem;
                    Assert.NotNull(counterGroup);

                    Assert.True(counterGroup.Values.TryGet(Values, out BlittableJsonReaderObject counterValues));
                    Assert.True(counterGroup.Values.TryGet(CounterNames, out BlittableJsonReaderObject counterNames));

                    Assert.Equal(counterValues.Count, counterNames.Count);

                    BlittableJsonReaderObject.PropertyDetails p1 = default, p2 = default;
                    for (int i = 0; i < counterValues.Count; i++)
                    {
                        counterValues.GetPropertyByIndex(i, ref p1);
                        counterNames.GetPropertyByIndex(i, ref p2);

                        Assert.Equal(p1.Name, p2.Name);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Counters)]
        public async Task FixCountersShouldContinueFromWhereItStopped()
        {
            var rand = new Random(12345);
            List<string> docIdsToCorrupt = ["users/10", "users/11", "users/12", "users/13", "users/14", "users/15"];

            using var store = GetDocumentStore(new Options
            {
                RunInMemory = false
            });

            using (var session = store.OpenSession())
            {
                for (int i = 1; i <= 100; i++)
                {
                    session.Store(new User(), "users/" + i);
                }

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                for (int i = 1; i <= 100; i++)
                {
                    var countersFor = session.CountersFor("users/" + i);

                    for (int j = 0; j < 100; j++)
                    {
                        var strSize = (int)rand.NextInt64(5, 15);
                        var s = GenRandomString(rand, strSize);

                        countersFor.Increment(s, j);
                    }
                }

                session.SaveChanges();
            }

            var db = await GetDatabase(store.Database);

            foreach (var id in docIdsToCorrupt)
            {
                // deliberately corrupt counters group data
                CorruptCountersData(db, id);

                // verify that counters data is corrupted - we're missing a counter name
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var readTable = new Table(CountersSchema, context.Transaction.InnerTransaction);
                    TableValueReader tvr;
                    using (DocumentIdWorker.GetSliceFromId(context, id, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                    {
                        Assert.True(readTable.SeekOnePrimaryKeyPrefix(documentKeyPrefix, out tvr));
                    }

                    var data = GetCounterValuesData(context, ref tvr);
                    Assert.True(data.TryGet(Values, out BlittableJsonReaderObject counterValues));
                    Assert.True(data.TryGet(CounterNames, out BlittableJsonReaderObject counterNames));

                    Assert.NotEqual(counterValues.Count, counterNames.Count);
                    BlittableJsonReaderObject.PropertyDetails p = default;
                    counterValues.GetPropertyByIndex(0, ref p);
                    var firstCounter = p.Name;

                    Assert.False(counterNames.TryGet(firstCounter, out string _));
                }
            }

            // override LastCounterFixed in order to launch the FixCounters tool upon restart, 
            // and set the LastCounterFixed to "users/12" (docs that come before "users/12" should Not be fixed)

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                db.DocumentsStorage.SetLastFixedCounterKey(context, "users/12");
                tx.Commit();
            }

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var lastCounterFixed = DocumentsStorage.ReadLastFixedCounterKey(context.Transaction.InnerTransaction);
                Assert.Equal("users/12", lastCounterFixed);
            }

            // reload the database to launch FixCounters tool
            await ReloadDatabase(store);
            db = await GetDatabase(store.Database);

            // wait for FixCounters to complete
            Assert.True(await WaitForValueAsync(() =>
            {
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    return DocumentsStorage.ReadLastFixedCounterKey(context.Transaction.InnerTransaction) == CountersRepairTask.Completed;
                }
            }, expectedVal: true, timeout: 10_000));

            // assert that docs Before "users/12" still have corrupted counters, and that docs After "users/12" were fixed
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var readTable = new Table(CountersSchema, context.Transaction.InnerTransaction);

                foreach (var id in docIdsToCorrupt)
                {
                    TableValueReader tvr;
                    using (DocumentIdWorker.GetSliceFromId(context, id, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                    {
                        Assert.True(readTable.SeekOnePrimaryKeyPrefix(documentKeyPrefix, out tvr));
                    }

                    var data = GetCounterValuesData(context, ref tvr);
                    Assert.True(data.TryGet(Values, out BlittableJsonReaderObject counterValues));
                    Assert.True(data.TryGet(CounterNames, out BlittableJsonReaderObject counterNames));

                    if (string.Compare(id, "users/12", StringComparison.OrdinalIgnoreCase) <= 0)
                    {
                        // ids before "users/12" should still be corrupted

                        Assert.NotEqual(counterValues.Count, counterNames.Count);
                        BlittableJsonReaderObject.PropertyDetails p = default;
                        counterValues.GetPropertyByIndex(0, ref p);
                        var firstCounter = p.Name;

                        Assert.False(counterNames.TryGet(firstCounter, out string _));
                        continue;
                    }

                    // ids after "users/12" should be fixed

                    Assert.Equal(counterValues.Count, counterNames.Count);

                    BlittableJsonReaderObject.PropertyDetails p1 = default, p2 = default;
                    for (int i = 0; i < counterValues.Count; i++)
                    {
                        counterValues.GetPropertyByIndex(i, ref p1);
                        counterNames.GetPropertyByIndex(i, ref p2);

                        Assert.Equal(p1.Name, p2.Name);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Counters)]
        public async Task LastCounterFixedShouldBePersisted()
        {
            using var store = GetDocumentStore(new Options
            {
                RunInMemory = false
            });

            var db = await GetDatabase(store.Database);
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var lastCounterFixed = DocumentsStorage.ReadLastFixedCounterKey(context.Transaction.InnerTransaction);
                Assert.Equal(CountersRepairTask.Completed, lastCounterFixed);
            }

            await ReloadDatabase(store);
            db = await GetDatabase(store.Database);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var lastCounterFixed = DocumentsStorage.ReadLastFixedCounterKey(context.Transaction.InnerTransaction);
                Assert.Equal(CountersRepairTask.Completed, lastCounterFixed);
            }
        }

        private unsafe void CorruptCountersData(DocumentDatabase database, string id = null)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var readTable = new Table(CountersSchema, context.Transaction.InnerTransaction);
                TableValueReader tvr;
                if (id == null)
                {
                    id = "users/1";
                    tvr = readTable.ReadFirst(CountersSchema.FixedSizeIndexes[AllCountersEtagSlice]).Reader;
                }
                else
                {
                    using (DocumentIdWorker.GetSliceFromId(context, id, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                    {
                        Assert.True(readTable.SeekOnePrimaryKeyPrefix(documentKeyPrefix, out tvr));
                    }
                }

                BlittableJsonReaderObject data;
                using (data = GetCounterValuesData(context, ref tvr))
                {
                    data = data.Clone(context);
                }

                data.TryGet(CounterNames, out BlittableJsonReaderObject originalNames);

                // remove first name from @names blittable
                originalNames.Modifications = new DynamicJsonValue(originalNames)
                {
                    Removals = [0]
                };

                using (var old = data)
                {
                    data = context.ReadObject(data, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                }

                using var changeVector = DocumentsStorage.TableValueToString(context, (int)CountersTable.ChangeVector, ref tvr);
                var groupEtag = DocumentsStorage.TableValueToEtag((int)CountersTable.Etag, ref tvr);

                using (var counterGroupKey = DocumentsStorage.TableValueToString(context, (int)CountersTable.CounterKey, ref tvr))
                using (context.Allocator.Allocate(counterGroupKey.Size, out var buffer))
                {
                    counterGroupKey.CopyTo(buffer.Ptr);

                    using (var clonedKey = context.AllocateStringValue(null, buffer.Ptr, buffer.Length))
                    using (Slice.External(context.Allocator, clonedKey, out var countersGroupKey))
                    using (Slice.From(context.Allocator, changeVector, out var cv))
                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, "Users", out _, out Slice collectionSlice))
                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext writeContext))
                    using (var tx = writeContext.OpenWriteTransaction())
                    {
                        var tableName = new CollectionName("Users").GetTableName(CollectionTableType.CounterGroups);
                        var writeTable = tx.InnerTransaction.OpenTable(CountersSchema, tableName);
                        using (writeTable.Allocate(out TableValueBuilder tvb))
                        {
                            tvb.Add(countersGroupKey);
                            tvb.Add(Bits.SwapBytes(groupEtag));
                            tvb.Add(cv);
                            tvb.Add(data.BasePointer, data.Size);
                            tvb.Add(collectionSlice);
                            tvb.Add(writeContext.GetTransactionMarker());

                            writeTable.Set(tvb);
                        }

                        tx.Commit();
                    }
                }
            }
        }

        private static void AddMoreCountersForDoc(IDocumentStore store, Random rand, string id, int numOfIterations = 100, int replicas = 0)
        {
            for (int i = 0; i < numOfIterations; i++)
            {
                using (var session = store.OpenSession())
                {
                    var countersFor = session.CountersFor(id);

                    for (int j = 0; j < 100; j++)
                    {
                        var strSize = (int)rand.NextInt64(5, 15);
                        var counterName = GenRandomString(rand, strSize);

                        countersFor.Increment(counterName, j);
                    }

                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(60), replicas: replicas);

                    session.SaveChanges();
                }
            }
        }

        private async Task ReloadDatabase(DocumentStore store)
        {
            var result = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));

            Assert.True(result.Success);
            Assert.True(result.Disabled);
            Assert.Equal(store.Database, result.Name);

            //wait until disabled databases unload, this is an immediate operation
            Assert.True(await WaitUntilDatabaseHasState(store, TimeSpan.FromSeconds(30), isLoaded: false));

            //now we enable all databases, so FixCounters tool should be launched
            result = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));

            Assert.True(result.Success);
            Assert.False(result.Disabled);
            Assert.Equal(store.Database, result.Name);

            Assert.True(await WaitUntilDatabaseHasState(store, TimeSpan.FromSeconds(10), db => db.Disabled == false));
        }
    }
}
