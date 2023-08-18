using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Graph;
using FastTests.Server.Replication;
using FastTests.Utils;
using Microsoft.AspNetCore.Http.HttpResults;
using NetTopologySuite.Utilities;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Assert = Xunit.Assert;

namespace SlowTests.Issues
{
    internal class RavenDB_20731 : ReplicationTestBase
    {
        public RavenDB_20731(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ForceCreatedRevisionsShouldReplicate()
        {
            using var store1 = GetDocumentStore();
            using var store2 = GetDocumentStore();

            var configuration = new RevisionsConfiguration
            {
                Default = null,
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>()
                {
                    ["Comments"] = new RevisionsCollectionConfiguration { Disabled = false, PurgeOnDelete = true, },
                    ["Products"] = new RevisionsCollectionConfiguration { Disabled = false, PurgeOnDelete = true, }
                }
            };
            await RevisionsHelper.SetupRevisions(store1, Server.ServerStore, configuration: configuration);
            await RevisionsHelper.SetupRevisions(store2, Server.ServerStore, configuration: configuration);

            await SetupReplicationAsync(store1, store2);

            var userId = "Users/1";
            using (var session = store1.OpenAsyncSession())
            {
                var user = new User { Id = userId, Name = "Bob" };
                await session.StoreAsync(user);
                await session.SaveChangesAsync();

                session.Advanced.Revisions.ForceRevisionCreationFor(id: userId);
                await session.SaveChangesAsync(); // replicated because it changes the do and its cv (it adds to the doc "HasRevisions" flag), so it sent with the updated doc (with the flag) in the same batch.
            }

            using (var session = store1.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(userId);
                user.Name = "Alice";
                await session.SaveChangesAsync();
            }


            using (var session = store1.OpenAsyncSession())
            {
                session.Advanced.Revisions.ForceRevisionCreationFor(id: userId);
                await session.SaveChangesAsync(); // revision isn't replicated.
                                                  // it is in its own batch and its cv equals (equals or lower) to the previous batch (highest) cv - the cv of "Alice" (that has been sent in the previous batch).
                                                  // so the force-created revision is being skipped and not replicating to the other node.
            }

            await EnsureReplicatingAsync((DocumentStore)store1, (DocumentStore)store2);
            
            using (var session = store2.OpenAsyncSession())
            {
                var count = await session.Advanced.Revisions.GetCountForAsync(id: userId);
                Assert.Equal(2, count);
            }
        }


        [Fact]
        public async Task ForceCreatedRevisionsShouldReplicate_InternalReplication()
        {
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, watcherCluster: true);
            var databaseName = GetDatabaseName();
            await CreateDatabaseInCluster(databaseName, 2, leader.WebUrl);

            using var leaderStore = GetStoreForServer(leader, databaseName);
            var someFollower = nodes.First(n => n.ServerStore.NodeTag != leader.ServerStore.NodeTag);
            using var followerStore = GetStoreForServer(someFollower, databaseName);

            var configuration = new RevisionsConfiguration
            {
                Default = null,
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>()
                {
                    ["Comments"] = new RevisionsCollectionConfiguration { Disabled = false, PurgeOnDelete = true, },
                    ["Products"] = new RevisionsCollectionConfiguration { Disabled = false, PurgeOnDelete = true, }
                }
            };
            await RevisionsHelper.SetupRevisions(leaderStore, leader.ServerStore, configuration: configuration);

            var userId = "Users/1";
            using (var session = leaderStore.OpenAsyncSession())
            {
                var user = new User { Id = userId, Name = "Bob" };
                await session.StoreAsync(user);
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);
                await session.SaveChangesAsync();

                session.Advanced.Revisions.ForceRevisionCreationFor(id: userId);
                await session.SaveChangesAsync(); // replicated because it changes the do and its cv (it adds to the doc "HasRevisions" flag), so it sent with the updated doc (with the flag) in the same batch.
            }

            using (var session = followerStore.OpenAsyncSession())
            {
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);
                var user = await session.LoadAsync<User>(userId);
                user.Name = "Alice";
                await session.SaveChangesAsync();
            }
            
            using (var session = followerStore.OpenAsyncSession())
            {
                session.Advanced.Revisions.ForceRevisionCreationFor(id: userId);
                await session.SaveChangesAsync(); // revision isn't replicated.
                                                  // it is in its own batch and its cv equals (equals or lower) to the previous batch (highest) cv - the cv of "Alice" (that has been sent in the previous batch).
                                                  // so the force-created revision is being skipped and not replicating to the other node.
            }

            await EnsureReplicatingAsync((DocumentStore)followerStore, (DocumentStore)leaderStore);

            using (var session = leaderStore.OpenAsyncSession())
            {
                var count = await session.Advanced.Revisions.GetCountForAsync(id: userId);
                Assert.Equal(2, count);
            }
        }

        private IDocumentStore GetStoreForServer(RavenServer server, string database)
        {
            return new DocumentStore
                {
                    Database = database, 
                    Urls = new[] { server.WebUrl }, 
                    Conventions = new DocumentConventions { DisableTopologyUpdates = true }
                }.Initialize();
        }

    }
}
