using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Json;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class RavenDB_21466 : ReplicationTestBase
    {
        public RavenDB_21466(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task MissingRevisionTombstone()
        {
            var cluster = await CreateRaftCluster(2, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
                   {
                       Server = cluster.Leader,
                       ReplicationFactor = 1
                   }))
            {
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration()
                }));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete("foo/bar");
                    await session.SaveChangesAsync();
                }

                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        PurgeOnDelete = true
                    }
                }));

                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges();
                    await session.StoreAsync(new User(), "foo/bar2");
                    await session.SaveChangesAsync();
                }

                foreach (var server in Servers)
                {
                    var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    using(database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var count = database.DocumentsStorage.RevisionsStorage.GetRevisionsBinEntries(context, 0, long.MaxValue).Count();
                        Assert.Equal(1, count);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.TimeSeries)]
        public async Task MissingTimeSeriesSnapshot()
        {
            var cluster = await CreateRaftCluster(2, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
                   {
                       Server = cluster.Leader,
                       ReplicationFactor = 1,
                   }))
            {
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration()
                }));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "foo/bar");
                    session.TimeSeriesFor("foo/bar", "likes").Append(DateTime.UtcNow, 1);
                    session.TimeSeriesFor("foo/bar", "views").Append(DateTime.UtcNow, 2);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete("foo/bar");
                    await session.SaveChangesAsync();
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));

                await WaitAndAssertForValueAsync(async () => await GetMembersCount(store), 2);

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges();
                    await session.StoreAsync(new User(), "foo/bar2");
                    await session.SaveChangesAsync();
                }
                
                await Task.Delay(3000);

                var toRemove = record.Topology.AllNodes.Contains("A") ? "A" : "B";
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: true, fromNode: toRemove));

                using (var session = store.OpenAsyncSession())
                {
                    var rv = await session.Advanced.Revisions.GetForAsync<User>( "foo/bar");
                    // TODO: Assert.Equal(2, rv.Count);
                    Assert.Equal(4, rv.Count);

                    var metadatas = rv.Select(c => (MetadataAsDictionary)session.Advanced.GetMetadataFor(c)).ToList();

                    Assert.True(metadatas[1].ContainsKey(Constants.Documents.Metadata.RevisionTimeSeries));
                    Assert.True(metadatas[2].ContainsKey(Constants.Documents.Metadata.RevisionTimeSeries));
                }

            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Counters)]
        public async Task MissingCounterSnapshot()
        {
            var cluster = await CreateRaftCluster(2, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
                   {
                       Server = cluster.Leader,
                       ReplicationFactor = 1,
                   }))
            {
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration()
                }));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "foo/bar");
                    session.CountersFor("foo/bar").Increment("likes", 1);
                    session.CountersFor("foo/bar").Increment("views", 1);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete("foo/bar");
                    await session.SaveChangesAsync();
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));

                await WaitAndAssertForValueAsync(async () => await GetMembersCount(store), 2);

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges();
                    await session.StoreAsync(new User(), "foo/bar2");
                    await session.SaveChangesAsync();
                }

                await Task.Delay(3000);

                var toRemove = record.Topology.AllNodes.Contains("A") ? "A" : "B";
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: true, fromNode: toRemove));

                using (var session = store.OpenAsyncSession())
                {
                    var rv = await session.Advanced.Revisions.GetForAsync<User>( "foo/bar");
                    // TODO: Assert.Equal(2, rv.Count);
                    Assert.Equal(3, rv.Count);

                    var metadatas = rv.Select(c => (MetadataAsDictionary)session.Advanced.GetMetadataFor(c)).ToList();

                    Assert.True(metadatas[1].ContainsKey(Constants.Documents.Metadata.RevisionCounters));
                }
            }
        }

    }
}
