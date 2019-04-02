using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Counters
{
    public class CountersReplication : ReplicationTestBase
    {
        [Fact]
        public async Task ConflictsInMetadata()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1-A");
                    await session.SaveChangesAsync();
                }

                EnsureReplicating(storeA, storeB);

                await storeB.Operations.SendAsync(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "likes",
                                    Delta = 14
                                },
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "dislikes",
                                    Delta = 13
                                }
                            }
                        }
                    }
                }));

                await storeA.Operations.SendAsync(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "likes",
                                    Delta = 12
                                },
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "cats",
                                    Delta = 11
                                }
                            }
                        }
                    }
                }));

                EnsureReplicating(storeA, storeB);

                var val = storeB.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(26, val);

                val = storeB.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "dislikes" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(13, val);

                val = storeB.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "cats" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(11, val);

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1-A");
                    var counters = (object[])session.Advanced.GetMetadataFor(user)["@counters"];

                    Assert.Equal(3, counters.Length);
                    // verify that counters are sorted
                    Assert.Equal("cats", counters[0]);
                    Assert.Equal("dislikes", counters[1]);
                    Assert.Equal("likes", counters[2]);

                }
            }
        }

        [Fact]
        public async Task MergeCountersOnDocumentConflict()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", 10);
                    await session.SaveChangesAsync();
                }
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.CountersFor("users/1").Increment("dislikes", 10);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                var counters = storeB.Operations
                    .Send(new GetCountersOperation("users/1"));
                Assert.Equal(2, counters.Counters.Count);
                Assert.Equal("dislikes", counters.Counters[0].CounterName);
                Assert.Equal(10, counters.Counters[0].TotalValue);
                Assert.Equal("likes", counters.Counters[1].CounterName);
                Assert.Equal(10, counters.Counters[1].TotalValue);

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    var flags = session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.Flags];
                    var list = session.Advanced.GetCountersFor(user);
                    Assert.Equal((DocumentFlags.HasCounters | DocumentFlags.HasRevisions | DocumentFlags.Resolved).ToString(), flags);
                    Assert.Equal(2, list.Count);
                }
            }
        }

        [Fact]
        public async Task MergeCountersOnCounterConflict()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", 10);
                    await session.SaveChangesAsync();
                }
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.CountersFor("users/1").Increment("dislikes", 10);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                var counters = storeB.Operations
                    .Send(new GetCountersOperation("users/1"));
                Assert.Equal(2, counters.Counters.Count);
                Assert.Equal("dislikes", counters.Counters[0].CounterName);
                Assert.Equal(10, counters.Counters[0].TotalValue);
                Assert.Equal("likes", counters.Counters[1].CounterName);
                Assert.Equal(10, counters.Counters[1].TotalValue);

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");

                    var flags = session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.Flags];
                    Assert.Equal((DocumentFlags.HasCounters).ToString(), flags);
                    var list = session.Advanced.GetCountersFor(user);
                    Assert.Equal(2, list.Count);
                }
            }
        }

        [Fact]
        public async Task IncrementOnConflictedDocument()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1");
                    await session.SaveChangesAsync();
                }
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", 10);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                WaitUntilHasConflict(storeB, "users/1");

                using (var session = storeB.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("likes", 10);
                    await session.SaveChangesAsync();
                }

                var counter = storeB.Operations
                    .Send(new GetCountersOperation("users/1", new[] {"likes"}));
                Assert.Equal(1, counter.Counters.Count);
                Assert.Equal(20, counter.Counters[0].TotalValue);
            }
        }

        [Fact]
        public async Task PutNewCounterOnConflictedDocument()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", 10);
                    await session.SaveChangesAsync();
                }
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", 10);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                WaitUntilHasConflict(storeB, "users/1");

                using (var session = storeB.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("likes", 10);
                    session.CountersFor("users/1").Increment("dislikes", 10);
                    await session.SaveChangesAsync();
                }

                var counter = storeB.Operations
                    .Send(new GetCountersOperation("users/1"));
                Assert.Equal(2, counter.Counters.Count);
                Assert.Equal(10, counter.Counters[0].TotalValue);
                Assert.Equal(30, counter.Counters[1].TotalValue);

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Resolved"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    var list = session.Advanced.GetCountersFor(user);
                    Assert.Equal(2, list.Count);
                }
            }
        }

        [Fact]
        public async Task CounterTombstonesReplication()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Aviv1"
                    }, "users/1-A");

                    session.CountersFor("users/1-A").Increment("likes", 10);

                    await session.SaveChangesAsync();
                }

                WaitForDocument(storeB, "users/1-A");

                using (var session = storeB.OpenAsyncSession())
                {
                    session.CountersFor("users/1-A").Delete("likes");

                    await session.SaveChangesAsync();
                }

                EnsureReplicating(storeA, storeB);
                EnsureReplicating(storeB, storeA);

                Assert.Equal(0, storeB.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes" }))
                    .Counters.Count);

                Assert.Equal(0, storeA.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes" }))
                    .Counters.Count);
            }
        }

        [Fact]
        public async Task CounterTombstonesReplication1()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1-A");
                    await session.StoreAsync(new User { Name = "Aviv2" }, "users/2-A");

                    session.CountersFor("users/1-A").Increment("likes", 10);
                    session.CountersFor("users/1-A").Increment("dislikes", 20);
                    session.CountersFor("users/2-A").Increment("downloads", 30);

                    await session.SaveChangesAsync();
                }

                EnsureReplicating(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    session.CountersFor("users/1-A").Delete("likes");
                    session.CountersFor("users/2-A").Delete("downloads");

                    await session.SaveChangesAsync();
                }

                EnsureReplicating(storeA, storeB);

                Assert.Equal(0, storeB.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes" }))
                    .Counters.Count);

                var val = storeB.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "dislikes" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(20, val);

                Assert.Equal(0, storeB.Operations
                    .Send(new GetCountersOperation("users/2-A", new[] { "downloads" }))
                    .Counters.Count);

                var db = await GetDocumentDatabaseInstanceFor(storeB);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.GetTombstonesFrom(ctx, 0, 0, int.MaxValue).ToList();
                    Assert.Equal(2, tombstones.Count);
                    Assert.Equal(Tombstone.TombstoneType.Counter, tombstones[0].Type);
                    Assert.Equal(Tombstone.TombstoneType.Counter, tombstones[1].Type);
                }
            }
        }

        [Fact]
        public async Task CounterTombstonesReplication2()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1-A");
                    await session.StoreAsync(new User { Name = "Aviv2" }, "users/2-A");

                    session.CountersFor("users/1-A").Increment("likes", 10);
                    session.CountersFor("users/1-A").Increment("dislikes", 20);
                    session.CountersFor("users/2-A").Increment("downloads", 30);

                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    session.CountersFor("users/1-A").Delete("likes");
                    session.CountersFor("users/2-A").Delete("downloads");

                    await session.SaveChangesAsync();
                }

                // we intentionally setup the replication after counters were 
                // deleted from storage and counter tombstones were created 

                await SetupReplicationAsync(storeA, storeB);         
                EnsureReplicating(storeA, storeB);

                Assert.Equal(0, storeB.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes" }))
                    .Counters.Count);

                var val = storeB.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "dislikes" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(20, val);

                Assert.Equal(0, storeB.Operations
                    .Send(new GetCountersOperation("users/2-A", new[] { "downloads" }))
                    .Counters.Count);

                var db = await GetDocumentDatabaseInstanceFor(storeB);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.GetTombstonesFrom(ctx, 0, 0, int.MaxValue).ToList();
                    Assert.Equal(2, tombstones.Count);
                    Assert.Equal(Tombstone.TombstoneType.Counter, tombstones[0].Type);
                    Assert.Equal(Tombstone.TombstoneType.Counter, tombstones[1].Type);
                }
            }
        }

        [Fact]
        public async Task RestoreAndReplicateCounters()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var server = GetNewServer(new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = 1.ToString()
            }))
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    Server = server
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    Server = server,
                    CreateDatabase = false
                }))
                using (var store3 = GetDocumentStore(new Options
                {
                    Server = server
                }))
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        await session.StoreAsync(new User { Name = "Name2" }, "users/2");
                        session.CountersFor("users/1").Increment("likes", 100);
                        session.CountersFor("users/2").Increment("downloads", 500);

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        // need to be in a different transaction in order to split the replication into batches
                        session.CountersFor("users/1").Increment("dislikes", 200);
                        await session.SaveChangesAsync();
                    }

                    var config = new PeriodicBackupConfiguration
                    {
                        BackupType = BackupType.Backup,
                        LocalSettings = new LocalSettings
                        {
                            FolderPath = backupPath
                        },
                        IncrementalBackupFrequency = "* * * * *" //every minute
                    };

                    var backupTaskId = (await store1.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                    await store1.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));

                    var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(backupTaskId);
                    SpinWait.SpinUntil(() =>
                    {
                        var getPeriodicBackupResult = store1.Maintenance.Send(getPeriodicBackupStatus);
                        return getPeriodicBackupResult.Status?.LastEtag > 0;
                    }, TimeSpan.FromSeconds(15));

                    var restore = new RestoreBackupOperation(new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = store2.Database,
                    });

                    var result = await store2.Maintenance.Server.SendAsync(restore);
                    result.WaitForCompletion();

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(3, stats.CountOfCounters);

                    using (var session = store2.OpenAsyncSession())
                    {
                        await AssertCounters(session);
                    }

                    await SetupReplicationAsync(store2, store3);

                    using (var session = store2.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "marker");
                        await session.SaveChangesAsync();
                    }

                    Assert.NotNull(WaitForDocumentToReplicate<User>(store3, "marker", 10_000));

                    using (var session = store3.OpenAsyncSession())
                    {
                        await AssertCounters(session);
                    }
                }
            }
        }

        private static async Task AssertCounters(IAsyncDocumentSession session)
        {
            var user1 = await session.LoadAsync<User>("users/1");
            var user2 = await session.LoadAsync<User>("users/2");

            Assert.Equal("Name1", user1.Name);
            Assert.Equal("Name2", user2.Name);

            var dic = await session.CountersFor(user1).GetAllAsync();
            Assert.Equal(2, dic.Count);
            Assert.Equal(100, dic["likes"]);
            Assert.Equal(200, dic["dislikes"]);

            var val = await session.CountersFor(user2).GetAsync("downloads");
            Assert.Equal(500, val);
        }
    }
}
