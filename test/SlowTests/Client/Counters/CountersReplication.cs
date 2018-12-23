using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.ServerWide;
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
    }
}
