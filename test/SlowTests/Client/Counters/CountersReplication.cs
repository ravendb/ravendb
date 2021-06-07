using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents.Counters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Counters
{
    public class CountersReplication : ReplicationTestBase
    {
        public CountersReplication(ITestOutputHelper output) : base(output)
        {
        }

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
        public async Task CounterConflictBetweenNewAndDeleted()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", 100000);
                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Delete("likes");
                    await session.SaveChangesAsync();
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.CountersFor("users/1").Increment("dislikes", 100000);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                EnsureReplicating(storeA, storeB);
                
                using (var session = storeB.OpenAsyncSession())
                {
                    var counters = await session.CountersFor("users/1").GetAllAsync();
                    var counter = counters.First();
                    Assert.Equal("dislikes", counter.Key);
                    Assert.Equal(100000, counter.Value);
                }

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);
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
                    await session.StoreAsync(new User { Name = "Aviv" }, "users/1");
                    session.CountersFor("users/1").Increment("Likes", 10);
                    await session.SaveChangesAsync();
                }
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.CountersFor("users/1").Increment("Dislikes", 10);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                var counters = storeB.Operations
                    .Send(new GetCountersOperation("users/1"));
                Assert.Equal(2, counters.Counters.Count);
                Assert.Equal("Dislikes", counters.Counters[0].CounterName);
                Assert.Equal(10, counters.Counters[0].TotalValue);
                Assert.Equal("Likes", counters.Counters[1].CounterName);
                Assert.Equal(10, counters.Counters[1].TotalValue);

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    var flags = session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.Flags];
                    var list = session.Advanced.GetCountersFor(user);
                    Assert.Equal((DocumentFlags.HasCounters | DocumentFlags.HasRevisions | DocumentFlags.Resolved).ToString(), flags);
                    Assert.Equal(2, list.Count);
                    Assert.Equal("Dislikes", list[0]);
                    Assert.Equal("Likes", list[1]);

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
                    session.CountersFor("users/1").Increment("Likes", 10);
                    await session.SaveChangesAsync();
                }
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.CountersFor("users/1").Increment("Likes", 10);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                WaitUntilHasConflict(storeB, "users/1");

                using (var session = storeB.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("Likes", 10);
                    session.CountersFor("users/1").Increment("Dislikes", 10);
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
                    Assert.Equal("Dislikes", list[0]);
                    Assert.Equal("Likes", list[1]);
                }
            }
        }

        [Fact]
        public async Task DeleteCounterOnConflictedDocument()
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
                    await session.StoreAsync(new User { Name = "Aviv" }, "users/1");
                    session.CountersFor("users/1").Increment("Likes", 10);
                    await session.SaveChangesAsync();
                }
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.CountersFor("users/1").Increment("Likes", 10);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                WaitUntilHasConflict(storeB, "users/1");

                using (var session = storeB.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Delete("Likes");
                    session.CountersFor("users/1").Increment("Dislikes", 10);
                    await session.SaveChangesAsync();
                }

                var counter = storeB.Operations
                    .Send(new GetCountersOperation("users/1"));

                Assert.Equal(1, counter.Counters.Count);
                Assert.Equal(10, counter.Counters[0].TotalValue);

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
                    Assert.Equal(1, list.Count);
                    Assert.Equal("Dislikes", list[0]);
                }
            }
        }

        [Fact]
        public async Task CanSplitCounterWithUnicode()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    var counter = session.CountersFor("users/1");
                    for (int i = 0; i < 3000; i++)
                    {
                        counter.Increment($"⭐{i}");
                    }
                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public async Task CanHandleIncomingCounterReplicationWhenCounterGroupDocumentsAreSplitDifferently()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv" }, "users/1-A");
                    await session.SaveChangesAsync();
                }

                // put counters 'likes1', 'likes2', ... , 'likes999'
                // on users/1-A document in store A

                var ops = new List<CounterOperation>();
                for (int i = 0; i < 1000; i++)
                {
                    ops.Add(new CounterOperation
                    {
                        Type = CounterOperationType.Increment,
                        CounterName = "likes" + i,
                        Delta = i
                    });
                }
              
                await storeA.Operations.SendAsync(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = ops
                        }
                    }
                }));

                ops.Clear();

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv" }, "users/1-A");
                    await session.SaveChangesAsync();
                }

                // put counters 'likes1', 'likes101', ... , 'likes901'
                // on users/1-A document in store B

                var delta = 9999;
                for (int i = 0; i < 10; i++)
                {
                    var id = i * 100 + 1;

                    ops.Add(new CounterOperation
                    {
                        Type = CounterOperationType.Increment,
                        CounterName = "likes" + id,
                        Delta = delta
                    });
                }

                await storeB.Operations.SendAsync(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = ops
                        }
                    }
                }));

                // set up replication from storeB to storeA

                await SetupReplicationAsync(storeB, storeA);
                EnsureReplicating(storeB, storeA);


                // assert counter values

                for (int i = 0; i < 10; i++)
                {
                    var id = i * 100 + 1;
                    var val = storeA.Operations
                        .Send(new GetCountersOperation("users/1-A", new[] { "likes" + id  }))
                        .Counters[0]?.TotalValue;
                    Assert.Equal(id + delta, val);
                }
            }
        }

        [Fact]
        public async Task RestoreAndConnectTwoNodesShouldHaveSameCounterValue()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore(new Options
            {
                CreateDatabase = false
            }))
            using (var store3 = GetDocumentStore(new Options
            {
                CreateDatabase = false
            }))
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Name1"}, "users/1");
                    await session.StoreAsync(new User {Name = "Name2"}, "users/2");
                    session.CountersFor("users/1").Increment("likes", 100);
                    session.CountersFor("users/2").Increment("downloads", 500);
                    session.CountersFor("users/1").Increment("dislikes", 200);

                    await session.SaveChangesAsync();
    }

                var config = Backup.CreateBackupConfiguration(backupPath);
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store1);
                await Restore(backupPath, store2);
                await Restore(backupPath, store3);

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

        [Fact]
        public async Task RestoreAndReplicateCounters()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var server = GetNewServer(
                new ServerCreationOptions
                {
                    CustomSettings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = 1.ToString()                
                    },
                    RegisterForDisposal = false
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

                    var config = Backup.CreateBackupConfiguration(backupPath);
                    await Backup.UpdateConfigAndRunBackupAsync(server, config, store1);
                    await Restore(backupPath, store2);

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(2, stats.CountOfCounterEntries);

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

        [Fact]
        public async Task RespectTxBoundaries()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var database1 = await GetDocumentDatabaseInstanceFor(storeA);
                using (var controller = new ReplicationController(database1))
                {
                    using (var session = storeA.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        session.CountersFor("users/1").Increment("likes", 100);
                        await session.StoreAsync(new User { Name = "Name2" }, "users/2");
                        session.CountersFor("users/2").Increment("downloads", 500);
                        await session.SaveChangesAsync();
                    }

                    await SetupReplicationAsync(storeA, storeB);
                    controller.ReplicateOnce();

                    WaitForDocument(storeB, "users/1");

                    using (var session = storeB.OpenAsyncSession())
                    {
                        Assert.NotNull(await session.LoadAsync<User>("users/2"));
                    }
                }
            }
        }

        [Fact]
        public async Task CountersTotalValueCanOverflow()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", long.MaxValue / 2);
                    await session.SaveChangesAsync();
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", long.MaxValue / 2 + 2);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);

                Assert.True(WaitForDocument(storeB, "users/1"));

                EnsureReplicating(storeA, storeB);

                using (var session = storeB.OpenAsyncSession())
                {
                    var ex = await Assert.ThrowsAsync<CounterOverflowException>(async () => await session.CountersFor("users/1").GetAsync("likes"));
                    Assert.Contains("Overflow detected in counter 'likes' from document 'users/1'", ex.Message);

                    // but we will allow to decrement the partial counter value nevertheless
                    session.CountersFor("users/1").Increment("likes", long.MaxValue / 4);
                    ex = await Assert.ThrowsAsync<CounterOverflowException>(async () => await session.SaveChangesAsync());
                    Assert.Contains("Overflow detected in counter 'likes' from document 'users/1'", ex.Message);
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    // but we will allow only to _decrement_ the partial counter value nevertheless
                    session.CountersFor("users/1").Increment("likes", - long.MaxValue / 4);
                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    session.CountersFor("users/1").Increment("likes", long.MaxValue / 4);
                    await session.SaveChangesAsync();
                }

                EnsureReplicating(storeA, storeB);
            }
        }

        [Fact]
        public async Task CountersTotalValueCanOverflow2()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", long.MaxValue / 3);
                    await session.SaveChangesAsync();
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", long.MaxValue / 3);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", long.MaxValue / 2);
                    await session.SaveChangesAsync();
                }

                EnsureReplicating(storeA, storeB);
            }
        }

        private static async Task Restore(string backupPath, DocumentStore restoreStore)
        {
            var restore = new RestoreBackupOperation(new RestoreBackupConfiguration
            {
                BackupLocation = Directory.GetDirectories(backupPath).First(), DatabaseName = restoreStore.Database,
            });

            var result = await restoreStore.Maintenance.Server.SendAsync(restore);
            result.WaitForCompletion();
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
