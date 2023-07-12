using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class ReplicationOfConflicts : ReplicationTestBase
    {
        public ReplicationOfConflicts(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicateAConflictThenResolveIt(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "foo/bar");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);

                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Length);

                await SetupReplicationAsync(store2, store1);

                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Length);

                // adding new document, resolve the conflict
                using (var session = store2.OpenSession())
                {
                    session.Store(new User { Name = "Resolved" }, "foo/bar");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument(store1, "foo/bar"));

                var resolvedConflicts = (await store1.Maintenance.SendAsync(new GetResolvedRevisionsOperation())).Results.ToList();
                Assert.Equal(1, resolvedConflicts.Count);

                var resolvedConflicts2 = (await store2.Maintenance.SendAsync(new GetResolvedRevisionsOperation())).Results.ToList();
                Assert.Equal(1, resolvedConflicts2.Count);

                Assert.Empty(store1.Commands().GetConflictsFor("foo/bar"));
                Assert.Empty(store2.Commands().GetConflictsFor("foo/bar"));
            }
        }

        public class GetResolvedRevisionsOperation : IMaintenanceOperation<ResolvedRevisions>
        {
            private readonly DateTime? _since;
            private readonly int? _take;

            public GetResolvedRevisionsOperation(DateTime? since = null, int? take = null)
            {
                _since = since;
                _take = take;
            }

            public RavenCommand<ResolvedRevisions> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetResolvedRevisionsCommand(_since, _take);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanManuallyResolveConflict_with_tombstone(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var master = GetDocumentStore(options: options))
            using (var slave = GetDocumentStore(options: options))
            {
                await SetupReplicationAsync(master, slave);

                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument(slave, "users/1"));

                using (var session = slave.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }
                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.Store(new User
                    {
                        Name = "Karmeli-2"
                    }, "final-marker");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument(slave, "final-marker"));

                using (var session = slave.OpenSession())
                {
                    try
                    {
                        session.Load<User>("users/1");
                        Assert.False(true, "Expected a confclit here");
                    }
                    catch (DocumentConflictException e)
                    {
                        Assert.Equal(e.DocId, "users/1");
                        Assert.NotEqual(e.LargestEtag, 0);
                        slave.Commands().Delete("users/1", null); //resolve conflict to the one with tombstone
                    }
                }

                using (var session = slave.OpenSession())
                {
                    //after resolving the conflict, should not throw
                    Assert.Null(session.Load<User>("users/1"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicateAConflictOnThreeDBsAndResolve(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var store1 = GetDocumentStore(options: options))
            using (var store2 = GetDocumentStore(options: options))
            using (var store3 = GetDocumentStore(options: options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "foo/bar");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2, store3);

                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Length);

                await SetupReplicationAsync(store2, store1);

                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Length);
                Assert.Equal(2, WaitUntilHasConflict(store3, "foo/bar").Length);

                using (var session = store2.OpenSession())
                {
                    session.Store(new User { Name = "Resolved" }, "foo/bar");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument(store1, "foo/bar"));
                Assert.True(WaitForDocument(store3, "foo/bar"));
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicateTombstoneConflict(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var store1 = GetDocumentStore(options: options))
            using (var store2 = GetDocumentStore(options: options))
            {
                await SetupReplicationAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument(store2, "foo/bar"));

                using (var session = store2.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocumentDeletion(store2, "foo/bar"));

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "foo/bar");
                    session.SaveChanges();
                }

                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Length);

                await SetupReplicationAsync(store2, store1);

                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Length);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ResolveHiLoConflict(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var store1 = GetDocumentStore(options: options))
            using (var store2 = GetDocumentStore(options: options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" });
                    session.SaveChanges();
                }
                using (var session = store2.OpenSession())
                {
                    session.Store(new User { Name = "Oren" });
                    session.SaveChanges();
                }
                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                Assert.Equal(2, WaitUntilHasConflict(store1, "users/1-A").Length);

                Assert.Equal(2, WaitUntilHasConflict(store2, "users/1-A").Length);

                var dbName = options.DatabaseMode == RavenDatabaseMode.Single ? store1.Database : await Sharding.GetShardDatabaseNameForDocAsync(store1, "users/1-A");
                var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    long etag;
                    using (ctx.OpenReadTransaction())
                    {
                        etag = db.DocumentsStorage.GetLastDocumentEtag(ctx.Transaction.InnerTransaction, CollectionName.HiLoCollection);
                    }
                    await Task.Delay(200); // twice the minimal heartbeat
                    using (ctx.OpenReadTransaction())
                    {
                        Assert.Equal(etag, db.DocumentsStorage.GetLastDocumentEtag(ctx.Transaction.InnerTransaction, CollectionName.HiLoCollection));
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task TombstoneAndDocumentConflictFromDifferentCollections(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            using (var store3 = GetDocumentStore(options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new Company() { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                using (var session = store2.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store2, store1);

                await SetupReplicationAsync(store1, store3);
                await SetupReplicationAsync(store2, store3);

                await SetupReplicationAsync(store3, store1);
                await SetupReplicationAsync(store3, store2);

                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store2, store3);
                await EnsureReplicatingAsync(store3, store1);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RemoveDeleteRevisionFromDifferentCollection(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            using (var store3 = GetDocumentStore(options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new Company() { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                using (var session = store3.OpenSession())
                {
                    session.Store(new Address { Street = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store3.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                using (var session = store2.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store2, store1);

                await SetupReplicationAsync(store1, store3);
                await SetupReplicationAsync(store2, store3);

                await SetupReplicationAsync(store3, store1);
                await SetupReplicationAsync(store3, store2);

                await EnsureReplicatingAsync(store2, store1);
                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store2, store3);
                await EnsureReplicatingAsync(store3, store1);

                var db1 = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "foo/bar");
                var token = new OperationCancelToken(TimeSpan.FromSeconds(60), CancellationToken.None, CancellationToken.None);
                await db1.DocumentsStorage.RevisionsStorage.EnforceConfiguration(onProgress: null, token);

                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store2, store3);
                await EnsureReplicatingAsync(store3, store1);
            }
        }
    }
}
