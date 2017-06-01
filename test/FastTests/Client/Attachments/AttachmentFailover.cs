using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Raven.Server.Documents;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client.Attachments
{
    public class AttachmentFailover : ClusterTestBase
    {
        [Theory(Skip = "Almost done")]
        [InlineData(512 * 1024)]
        public async Task PutAttachmentsWithFailoverUsingSession(long size)
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            using (var store = GetDocumentStore(defaultServer: leader, replicationFactor: 2))
            {
                using (var session = (DocumentSession)store.OpenSession())
                {
                    session.Store(new User {Name = "Fitzchak"}, "users/1");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        session,
                        "users/1",
                        u => u.Name.Equals("Fitzchak"),
                        TimeSpan.FromSeconds(10)));
                }

                using (var session = (DocumentSession)store.OpenSession())
                using (var stream = new BigDummyStream(size))
                {
                    session.Advanced.StoreAttachment("users/1", "File", stream, "application/pdf");

                    // SaveSession with failover
                    var saveChangesOperation = new BatchOperation(session);
                    using (var command = saveChangesOperation.CreateRequest())
                    {
                        var currentNode = await session.RequestExecutor.GetCurrentNode();
                        var task = session.RequestExecutor.ExecuteAsync(currentNode, session.Context, command);
                        var currentServer = Servers.Single(x => x.ServerStore.NodeTag == currentNode.ClusterTag);
                        DisposeServerAndWaitForFinishOfDisposal(currentServer);
                        try
                        {
                            await task;
                        }
                        catch (Exception)
                        {
                            // TODO: Make sure that we do not get an error here because of failing GetTcpInfo
                        }
                        saveChangesOperation.SetResult(command.Result);
                    }
                }

                using (var session = store.OpenSession())
                using (var stream = new BigDummyStream(size))
                {
                    var attachment = session.Advanced.GetAttachment("users/1", "File", (result, streamResult) => streamResult.CopyTo(stream));
                    Assert.Equal(2, attachment.Etag);
                    Assert.Equal("File", attachment.Name);
                    Assert.Equal(size, stream.Position);
                    Assert.Equal(size, attachment.Size);
                    Assert.Equal("application/pdf", attachment.ContentType);

                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal((DocumentFlags.HasAttachments | DocumentFlags.FromReplication).ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    var attachmentMetadata = attachments.Single();
                    Assert.Equal("File", attachmentMetadata.GetString(nameof(AttachmentResult.Name)));
                    Assert.Equal("application/pdf", attachmentMetadata.GetString(nameof(AttachmentResult.ContentType)));
                    Assert.Equal(size, attachmentMetadata.GetNumber(nameof(AttachmentResult.Size)));
                }
            }
        }

        [Theory(Skip = "TODO")]
        [InlineData(512 * 1024)]
        public async Task PutAttachmentsWithFailoverUsingCommand(long size)
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            using (var store = GetDocumentStore(defaultServer: leader, replicationFactor: 2))
            {
                using (var session = (DocumentSession)store.OpenSession())
                {
                    session.Store(new User {Name = "Fitzchak"}, "users/1");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        session,
                        "users/1",
                        u => u.Name.Equals("Fitzchak"),
                        TimeSpan.FromSeconds(10)));
                }

                using (var stream = new BigDummyStream(size))
                {
                    // TODO
                }

                using (var session = store.OpenSession())
                using (var stream = new BigDummyStream(size))
                {
                    var attachment = session.Advanced.GetAttachment("users/1", "File", (result, streamResult) => streamResult.CopyTo(stream));
                    Assert.Equal(2, attachment.Etag);
                    Assert.Equal("File", attachment.Name);
                    Assert.Equal(size, stream.Position);
                    Assert.Equal(size, attachment.Size);
                    Assert.Equal("application/pdf", attachment.ContentType);

                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal((DocumentFlags.HasAttachments | DocumentFlags.FromReplication).ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    var attachmentMetadata = attachments.Single();
                    Assert.Equal("File", attachmentMetadata.GetString(nameof(AttachmentResult.Name)));
                    Assert.Equal("application/pdf", attachmentMetadata.GetString(nameof(AttachmentResult.ContentType)));
                    Assert.Equal(size, attachmentMetadata.GetNumber(nameof(AttachmentResult.Size)));
                }
            }
        }
    }
}