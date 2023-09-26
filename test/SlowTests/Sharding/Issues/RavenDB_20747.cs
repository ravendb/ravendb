using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_20747 : ReplicationTestBase
    {
        public RavenDB_20747(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Sharding)]
        public async Task RevertRevisionsAfterResharding()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = false);

                var id = "users/shiran";
                DateTime last;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Shiran" }, id);
                    session.SaveChanges();
                    last = DateTime.UtcNow;
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    Assert.NotNull(user);
                    user.Name = "Shiran2";
                    session.SaveChanges();
                }

                var db0 = await GetDocumentDatabaseInstanceForAsync(store, RavenDatabaseMode.Sharded, id);

                RevertResult result;
                using (var token = new OperationCancelToken(db0.Configuration.Databases.OperationTimeout.AsTimeSpan, db0.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db0.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                Assert.Equal(2, result.ScannedRevisions);
                Assert.Equal(1, result.ScannedDocuments);
                Assert.Equal(1, result.RevertedDocuments);

                using (var session = store.OpenAsyncSession())
                {
                    var userRevisions = await session.Advanced.Revisions.GetForAsync<User>(id);
                    Assert.Equal(3, userRevisions.Count);

                    Assert.Equal("Shiran", userRevisions[0].Name);
                    Assert.Equal("Shiran2", userRevisions[1].Name);
                    Assert.Equal("Shiran", userRevisions[2].Name);
                }

                await Sharding.Resharding.MoveShardForId(store, id, toShard: 1);

                var db1 = await GetDocumentDatabaseInstanceForAsync(store, RavenDatabaseMode.Sharded, id);

                using (var token = new OperationCancelToken(db1.Configuration.Databases.OperationTimeout.AsTimeSpan, db1.DatabaseShutdown, CancellationToken.None))
                {
                    await db1.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                await Sharding.Resharding.MoveShardForId(store, id, toShard: 0);

                using (var session = store.OpenSession())
                {
                    session.Delete(id);
                    session.SaveChanges();
                }

                using (var token = new OperationCancelToken(db0.Configuration.Databases.OperationTimeout.AsTimeSpan, db0.DatabaseShutdown, CancellationToken.None))
                {
                    await db0.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                await Sharding.Resharding.MoveShardForId(store, id, toShard: 1);

                await Sharding.EnsureNoDatabaseChangeVectorLeakAsync(store.Database);
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions | RavenTestCategory.Sharding)]
        [RavenExternalReplication(RavenDatabaseMode.Sharded, RavenDatabaseMode.Single)]
        [RavenExternalReplication(RavenDatabaseMode.Sharded, RavenDatabaseMode.Sharded)]
        public async Task RevertRevisionsWithReplicationAndResharding(Options sourceOptions, Options destinationOptions)
        {
            using (var store1 = GetDocumentStore(sourceOptions))
            using (var store2 = GetDocumentStore(destinationOptions))
            {
                await RevisionsHelper.SetupRevisionsAsync(store1, modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = false);

                var id = "users/shiran";
                DateTime last;
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Shiran" }, id);
                    session.SaveChanges();
                    last = DateTime.UtcNow;
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                await EnsureReplicatingAsync(store1, store2);

                using (var session = store2.OpenSession())
                {
                    var user = session.Load<User>(id);
                    Assert.NotNull(user);
                    user.Name = "Shiran2";
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(store2, store1);

                var db1 = await GetDocumentDatabaseInstanceForAsync(store1, sourceOptions.DatabaseMode, id);

                using (var token = new OperationCancelToken(db1.Configuration.Databases.OperationTimeout.AsTimeSpan, db1.DatabaseShutdown, CancellationToken.None))
                {
                    await db1.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                await Sharding.Resharding.MoveShardForId(store1, id, toShard: 1);

                var db2 = await GetDocumentDatabaseInstanceForAsync(store2, destinationOptions.DatabaseMode, id);

                using (var token = new OperationCancelToken(db2.Configuration.Databases.OperationTimeout.AsTimeSpan, db2.DatabaseShutdown, CancellationToken.None))
                {
                    await db2.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                await Sharding.Resharding.MoveShardForId(store1, id, toShard: 0);

                using (var session = store1.OpenSession())
                {
                    session.Delete(id);
                    session.SaveChanges();
                }

                using (var token = new OperationCancelToken(db1.Configuration.Databases.OperationTimeout.AsTimeSpan, db1.DatabaseShutdown, CancellationToken.None))
                {
                    await db1.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null,
                        token: token);
                }

                await Sharding.Resharding.MoveShardForId(store1, id, toShard: 1);

                await Sharding.EnsureNoDatabaseChangeVectorLeakAsync(store1.Database);
            }
        }
    }
}
