using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Schemas;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;

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
            // use WaitForValue since replication convergence may take time after conflict resolution
            Assert.Equal(5, WaitForValue(() => GetTestCounter(storeA), 5L, timeout: 30_000));
            Assert.Equal(5, WaitForValue(() => GetTestCounter(storeB), 5L, timeout: 30_000));
            Assert.Equal(5, WaitForValue(() => GetTestCounter(storeC), 5L, timeout: 30_000));
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

        [RavenFact(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        public async Task FixCountersToolShouldFixDbIdsCorruption_OnDatabaseStart()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "RavenDB_24797.ravendb-snapshot");
            RavenDB_13468.ExtractFile(fullBackupPath, "SlowTests.Data.RavenDB_24797.24797_counters_test.ravendb-snapshot");

            using (var store = GetDocumentStore())
            {
                var databaseName = GetDatabaseName();

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupPath,
                    DatabaseName = databaseName
                }))
                {
                    var db = await GetDocumentDatabaseInstanceFor(store, databaseName);

                    // wait for FixCounters tool to complete
                    Assert.True(await WaitForValueAsync(() =>
                    {
                        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            return DocumentsStorage.ReadLastFixedCounterKey(context.Transaction.InnerTransaction) == CountersRepairTask.Completed;
                        }
                    }, expectedVal: true, timeout: 10_000));

                    // verify that counters data is not corrupted - no bad dbIds
                    var countersStorage = db.DocumentsStorage.CountersStorage;
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var countersDoc = countersStorage.GetCounterValuesForDocument(ctx, TestDocumentId).FirstOrDefault()?.Values;
                        Assert.NotNull(countersDoc);

                        // should not throw
                        countersDoc.BlittableValidation();

                        countersDoc.TryGet(CountersStorage.DbIds, out BlittableJsonReaderArray dbIds);

                        var validDbIds = dbIds.All(x => CountersRepairTask.IsBase64String(x as LazyStringValue));
                        Assert.True(validDbIds);
                    }
                }

            }
        }

        [RavenFact(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        public async Task FixCountersToolShouldFixDbIdsCorruption_WhenCalledManually()
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

                // verify that counters data is corrupted 
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var readTable = new Table(db.DocumentsStorage.CountersStorage.CountersSchema, context.Transaction.InnerTransaction);
                    TableValueReader tvr;
                    using (DocumentIdWorker.GetLoweredIdSliceFromId(context, id, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                    {
                        Assert.True(readTable.SeekOnePrimaryKeyPrefix(documentKeyPrefix, out tvr));
                    }

                    var data = CountersStorage.GetCounterValuesData(context, ref tvr);
                    data.TryGet(CountersStorage.DbIds, out BlittableJsonReaderArray dbIds);

                    var validDbIds = dbIds.All(x => CountersRepairTask.IsBase64String(x as LazyStringValue));
                    Assert.False(validDbIds);
                }
            }

            // call FixCounters tool
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var numOfFixes = db.CountersRepairTask.FixCountersForDocuments(context, docIdsToCorrupt, hasMore: false);
                Assert.Equal(docIdsToCorrupt.Count, numOfFixes);

                tx.Commit();
            }

            // calling FixCounters tool again should return 0 - no CounterGroup was fixed
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenWriteTransaction())
            {
                var numOfFixes = db.CountersRepairTask.FixCountersForDocuments(context, docIdsToCorrupt, hasMore: false);
                Assert.Equal(0, numOfFixes);
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

                    // should not throw
                    counterGroup.Values.BlittableValidation();

                    counterGroup.Values.TryGet(CountersStorage.DbIds, out BlittableJsonReaderArray dbIds);

                    var validDbIds = dbIds.All(x => CountersRepairTask.IsBase64String(x as LazyStringValue));
                    Assert.True(validDbIds);
                }
            }
        }

        [RavenFact(RavenTestCategory.Counters | RavenTestCategory.Replication)]
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

                // verify that counters data is corrupted 
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var readTable = new Table(db.DocumentsStorage.CountersStorage.CountersSchema, context.Transaction.InnerTransaction);
                    TableValueReader tvr;
                    using (DocumentIdWorker.GetLoweredIdSliceFromId(context, id, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                    {
                        Assert.True(readTable.SeekOnePrimaryKeyPrefix(documentKeyPrefix, out tvr));
                    }

                    var data = CountersStorage.GetCounterValuesData(context, ref tvr);
                    data.TryGet(CountersStorage.DbIds, out BlittableJsonReaderArray dbIds);

                    var validDbIds = dbIds.All(x => CountersRepairTask.IsBase64String(x as LazyStringValue));
                    Assert.False(validDbIds);
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

                    // should not throw
                    counterGroup.Values.BlittableValidation();

                    counterGroup.Values.TryGet(CountersStorage.DbIds, out BlittableJsonReaderArray dbIds);
                    
                    var validDbIds = dbIds.All(x => CountersRepairTask.IsBase64String(x as LazyStringValue));
                    Assert.True(validDbIds);
                }
            }
        }

        private const string TestCounterName = "someCounter";
        private const string TestDocumentId = "users/1-A";

        private unsafe void CorruptCountersData(DocumentDatabase database, string id = null)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var readTable = new Table(database.DocumentsStorage.CountersStorage.CountersSchema, context.Transaction.InnerTransaction);
                TableValueReader tvr;
                using (DocumentIdWorker.GetLoweredIdSliceFromId(context, id, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                {
                    Assert.True(readTable.SeekOnePrimaryKeyPrefix(documentKeyPrefix, out tvr));
                }

                BlittableJsonReaderObject data;
                using (data = CountersStorage.GetCounterValuesData(context, ref tvr))
                {
                    data = data.Clone(context);
                }

                data.TryGet(CountersStorage.DbIds, out BlittableJsonReaderArray dbIds);

                // add corrupted dbId
                var lsv = context.GetLazyString("\u0016IKVsIz2T0UusRoMwj8J+A");

                dbIds.Modifications = new DynamicJsonArray();
                dbIds.Modifications.Add(lsv);

                using (lsv)
                using (var old = data)
                {
                    data = context.ReadObject(data, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                }

                using var changeVector = DocumentsStorage.TableValueToString(context, (int)Counters.CountersTable.ChangeVector, ref tvr);
                var groupEtag = DocumentsStorage.TableValueToEtag((int)Counters.CountersTable.Etag, ref tvr);

                using (var counterGroupKey = DocumentsStorage.TableValueToString(context, (int)Counters.CountersTable.CounterKey, ref tvr))
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
                        var writeTable = tx.InnerTransaction.OpenTable(database.DocumentsStorage.CountersStorage.CountersSchema, tableName);
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

        private static long GetTestCounter(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                return session.CountersFor(TestDocumentId).Get(TestCounterName) ?? -1;
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
