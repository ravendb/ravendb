using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Revisions
{
    public class ReplicationRevisionsTests : ReplicationTestBase
    {
        public ReplicationRevisionsTests(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public async Task ReplicateRevision_WhenSourceDataFromReplicationAndDocDeleted_ShouldNotResuscitateTheDoc()
        {
            var exportFile = GetTempFileName();
            
            var (nodes, leader) = await CreateRaftCluster(2);
            var nodeTags = nodes.Select(n => n.ServerStore.NodeTag).ToArray();

            string firstNode;
            using (var store = GetDocumentStore(new Options {Server = leader, ReplicationFactor = 1}))
            {
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration{Default = new RevisionsCollectionConfiguration()}));
                firstNode = await AssertWaitForNotNullAsync(async () =>
                    (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).Topology.Members?.FirstOrDefault());

                var entity = new User();
                using (var session = store.OpenAsyncSession())
                {
                    //Add first revision with first node tag
                    await session.StoreAsync(entity);
                    await session.SaveChangesAsync();
                }
                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));
                await AssertWaitForValueAsync(async () => (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).Topology.Members?.Count, 2);
                
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true, nodeTags.First(n => n == firstNode)));
                await AssertWaitForValueAsync(async () =>
                {
                    var dbRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return dbRecord?.DeletionInProgress == null || dbRecord.DeletionInProgress.Count == 0;
                }, true);

                using (var session = store.OpenAsyncSession())
                {
                    //Add update revision with second node tag
                    entity.Name = "Changed";
                    await session.StoreAsync(entity);
                    await session.SaveChangesAsync();
                    
                    // Add delete revision with second node tag
                    session.Delete(entity.Id);
                    await session.SaveChangesAsync();
                }

                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
                await operation.WaitForCompletionAsync();
            }

            using (var store = GetDocumentStore(new Options {Server = leader, ReplicationFactor = 2}))
            {
                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
                await operation.WaitForCompletionAsync();

                using var re1 = RequestExecutor.CreateForSingleNodeWithConfigurationUpdates(nodes[0].WebUrl, store.Database, null, store.Conventions);
                using var re2 = RequestExecutor.CreateForSingleNodeWithConfigurationUpdates(nodes[1].WebUrl, store.Database, null, store.Conventions);
                using (var firstSession = store.OpenAsyncSession(new SessionOptions{RequestExecutor = re1}))
                using (var secondSession = store.OpenAsyncSession(new SessionOptions{RequestExecutor = re2}))
                {
                    WaitForIndexing(store, store.Database, nodeTag:nodes[0].ServerStore.NodeTag);
                    var firstNodeDocs = await firstSession.Query<User>().ToArrayAsync();
                    
                    WaitForIndexing(store, store.Database, nodeTag:nodes[1].ServerStore.NodeTag);
                    var secondNodeDocs = await secondSession.Query<User>().ToArrayAsync();

                    RavenTestHelper.AssertAll(
                        () => Assert.Equal(0, firstNodeDocs.Length),
                        () => Assert.Equal(0, secondNodeDocs.Length));
                }
            }
        }
    }
}