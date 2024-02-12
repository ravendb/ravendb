using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Xunit;
using Constants = Raven.Client.Constants;
using Xunit.Abstractions;
using Raven.Server.Rachis;
using Tests.Infrastructure;

namespace SlowTests.Issues
{
    public class RavenDB_17824 : ReplicationTestBase
    {
        public RavenDB_17824(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [RavenFact(RavenTestCategory.None)]
        public async Task DeleteDatabasesOperationShouldWaitForBeingAppliedInAllNodes()
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);
            var follower = nodes.Single(n => n != leader);

            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 });
            var config = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration() };
            await RevisionsHelper.SetupRevisions(store, leader.ServerStore, config);

            using var storeL = GetDocumentStoreForSpecificServer(leader, store.Database);
            using var storeF = GetDocumentStoreForSpecificServer(follower, store.Database);

            var entity = new User { Name = "Old" };
            using (var session = storeL.OpenAsyncSession())
            {
                await session.StoreAsync(entity);
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);
                await session.SaveChangesAsync();
            }

            // prevent from follower to get updated record
            var followerDb = await follower.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            var mre = new ManualResetEvent(false);
            followerDb.ForTestingPurposes = new DocumentDatabase.TestingStuff();
            followerDb.ForTestingPurposes.BeforeUpdateUnused = () =>
            {
                mre.WaitOne(5_000);
            };

            // delete database from leader node
            await storeL.Maintenance.Server.SendAsync(
                new DeleteDatabasesOperation(store.Database, true, leader.ServerStore.NodeTag, TimeSpan.FromSeconds(30)));

            Assert.True(await WaitForValueAsync(async () =>
            {
                var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                return res != null && res.Topology.Count == 1;
            }, true));

            Assert.Equal(0, await WaitForValueAsync(async () =>
            {
                var records = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                return records.DeletionInProgress.Count;
            }, 0));

            followerDb.ForTestingPurposes.BeforeUpdateUnused = null;

            // add leader node to the database again(now leaser has the same db with different dbId)
            await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database, leader.ServerStore.NodeTag));

            Assert.True(await WaitForValueAsync(async () =>
            {
                var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                return res != null && res.Topology.Members.Count == 2;
            }, true));

            await EnsureReplicatingAsync((DocumentStore)storeF, (DocumentStore)storeL);

            // update 'entity' in leader node - conflict (because follower didnt get the updated record,
            // so it doesnt has the upsated databases 'unused list',
            // and because of it - the follower doesnt remove the tag with leader old db id from the change-vector).
            // A:3-newDbId B:8 A:5-oldDbId
            // var newLeaderDbId = (await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database)).DbBase64Id;
            using (var session = storeL.OpenAsyncSession())
            {
                entity.Name = $"Change after adding again node {leader.ServerStore.NodeTag} ";
                await session.StoreAsync(entity);
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);//, timeout: TimeSpan.FromDays(1));
                await session.SaveChangesAsync();
            }

            using (var session = storeL.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<User>(entity.Id);
                var metadata = session.Advanced.GetMetadataFor(loaded);
                var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
                var info = "";
                var lastRevisionIsResolved = flags.Contains(DocumentFlags.Resolved.ToString());
                if (lastRevisionIsResolved)
                {
                    info += $"Flags: {flags}\n";
                    var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    info += $"Topology: \n";
                    foreach (var member in res.Topology.Members)
                    {
                        info += $"{member} \n";
                    }

                    var revisions = await session.Advanced.Revisions.GetForAsync<User>(entity.Id);
                    info += $"Revisions: \n";
                    foreach (var rev in revisions)
                    {
                        var ch = session.Advanced.GetChangeVectorFor(rev);
                        var fl = session.Advanced.GetMetadataFor(rev).GetString(Constants.Documents.Metadata.Flags);
                        info += $"{rev.Name} : {fl} - {ch} \n";
                    }
                    info += $"Flags: {flags}\n";
                    Assert.False(true, info);
                }
            }
        }


        [RavenFact(RavenTestCategory.None)]
        public async Task TwoDeleteRequestShouldThrow()
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);

            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 });
            var nodeToRemove = "B";

            // delete database from leader node
            var delete1 = store.Maintenance.Server.SendAsync(
                new DeleteDatabasesOperation(store.Database, true, nodeToRemove, TimeSpan.FromSeconds(30)));

            // delete database from leader node
            var delete2 = store.Maintenance.Server.SendAsync(
                new DeleteDatabasesOperation(store.Database, true, nodeToRemove, TimeSpan.FromSeconds(30)));

            // can't use Assert.Throws<T>(..) because it can be here Exception or RavenException,
            // and if you passes type 'Exception' and you get 'RavenException' it throws.
            Exception ex = null;
            try
            {
                await delete1;
                await delete2;
            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.NotNull(ex);

            var inner = ex.InnerException;
            var m1 = GetSecondDeleteExceptionMessege(store.Database, "A");
            var m2 = GetSecondDeleteExceptionMessege(store.Database, "B");


            if (ex is RavenException)
            {
                if (inner != null)
                {
                    Assert.True(inner is InvalidOperationException or RachisApplyException, $"inner exception should be of type 'InvalidOperationException' or 'RachisApplyException', but it is '{inner.GetType()}'");
                    Assert.True(ex.Message.Contains(m1) || ex.Message.Contains(m2));
                }
                else
                    Assert.True(ex.Message.Contains(m1) || ex.Message.Contains(m2));
            }
            else
            {
                if (inner != null)
                {
                    Assert.True(inner is RavenTimeoutException, $"inner exception should be of type 'RavenTimeoutException', but it is '{inner.GetType()}'");
                    Assert.True(ex.Message.Contains(m1) || ex.Message.Contains(m2), $"The message is: \"{ex.Message}\"");
                }
                else
                {
                    Assert.True(ex.Message.Contains(m1) || ex.Message.Contains(m2));
                }
            }
        }

        private string GetSecondDeleteExceptionMessege(string database, string nodeTag)
        {
            return $"Database '{database}' doesn't reside on node '{nodeTag}' so it can't be deleted from it";
        }
    }
}
