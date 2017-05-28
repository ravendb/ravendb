using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client.Attachments
{
    public class AttachmentFailover : ClusterTestBase
    {
        [Theory(Skip = "RavenDB-6987 - wait for the cluster work on the session to be done")]
        [InlineData(1024)]
        public async Task PutAttachmentsWithFailover(long size)
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            const int replicationFactor = 2;
            const string databaseName = nameof(PutAttachmentsWithFailover);
            using (var store = new DocumentStore
            {
                Database = databaseName,
                Urls = leader.WebUrls
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var databaseResult = store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));

                foreach (var server in Servers.Where(s => databaseResult.NodesAddedTo.Any(n => n == s.WebUrls[0])))
                {
                    // TODO: How do I wait for database to be created in the cluster?
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.ETag.Value);
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }

                using (var session = store.OpenSession(databaseName))
                {
                    session.Store(new User {Name = "Fitzchak"}, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    databaseResult.Topology,
                    "users/1",
                    u => u.Name.Equals("Fitzchak"),
                    TimeSpan.FromSeconds(10)));

                await ((DocumentStore)store).ForceUpdateTopologyFor(databaseName);

                using (var session = (DocumentSession)store.OpenSession(databaseName))
                using (var stream = new BigDummyStream(size))
                {
                    session.Advanced.StoreAttachment("users/1", "File", stream, "application/pdf");

                    var saveChangesOperation = new BatchOperation(session);
                    using (var command = saveChangesOperation.CreateRequest())
                    {
                        var currentNode = session.RequestExecutor.GetCurrentNode();
                        var task = session.RequestExecutor.ExecuteAsync(currentNode, session.Context, command);
                        var currentServer = Servers.Single(x => x.ServerStore.NodeTag == currentNode.ClusterTag);
                        DisposeServerAndWaitForFinishOfDisposal(currentServer);
                        await task;
                        saveChangesOperation.SetResult(command.Result);
                    }
                }

                using (var session = store.OpenSession(databaseName))
                using (var stream = new BigDummyStream(size))
                {
                    var attachment = session.Advanced.GetAttachment("users/1", "file", (result, streamResult) => streamResult.CopyTo(stream));
                    Assert.Equal(2, attachment.Etag);
                    Assert.Equal("File", attachment.Name);
                    Assert.Equal(size, stream.Position);
                    Assert.Equal(size, attachment.Size);
                    Assert.Equal("application/pdf", attachment.ContentType);
                }
            }
        }
    }
}