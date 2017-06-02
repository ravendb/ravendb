using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Raven.Server.Documents;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client.Attachments
{
    public class AttachmentFailover : ClusterTestBase
    {
        [Theory]
        [InlineData(true, 512 * 1024, "BfKA8g/BJuHOTHYJ+A6sOt9jmFSVEDzCM3EcLLKCRMU=")]
        [InlineData(false, 512 * 1024, "BfKA8g/BJuHOTHYJ+A6sOt9jmFSVEDzCM3EcLLKCRMU=")]
        public async Task PutAttachmentsWithFailover(bool useSession, long size, string hash)
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

                if (useSession)
                {
                    using (var session = (DocumentSession)store.OpenSession())
                    using (var stream = new BigDummyStream(size))
                    {
                        session.Advanced.StoreAttachment("users/1", "File", stream, "application/pdf");

                        // SaveSession with failover
                        var saveChangesOperation = new BatchOperation(session);
                        using (var command = saveChangesOperation.CreateRequest())
                        {
                            var currentNode = await session.RequestExecutor.GetCurrentNode();
                            var currentServer = Servers.Single(x => x.ServerStore.NodeTag == currentNode.ClusterTag);
                            var task = session.RequestExecutor.ExecuteAsync(currentNode, session.Context, command);
                            // We want to make sure that we started to write the stream and we set position zero before failing over
                            // This is why we dispose the server after the operation has started.
                            DisposeServerAndWaitForFinishOfDisposal(currentServer);
                            await task;
                            saveChangesOperation.SetResult(command.Result);
                        }
                        catch (Exception)
                        {
                            // TODO: Make sure that we do not get an error here because of failing GetTcpInfo
                        }
                        saveChangesOperation.SetResult(command.Result);
                    }
                }
                else
                {
                    var requestExecutor = store.GetRequestExecutor();
                    using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    using (var stream = new BigDummyStream(size))
                    {
                        var command = new PutAttachmentOperation("users/1", "File", stream, "application/pdf")
                            .GetCommand(store.Conventions, context, requestExecutor.Cache);

                        var currentNode = await requestExecutor.GetCurrentNode();
                        var currentServer = Servers.Single(x => x.ServerStore.NodeTag == currentNode.ClusterTag);
                        var task = requestExecutor.ExecuteAsync(currentNode, context, command);
                        // We want to make sure that we started to write the stream and we set position zero before failing over
                        // This is why we dispose the server after the operation has started.
                        DisposeServerAndWaitForFinishOfDisposal(currentServer);
                        await task;
                        var attachment = command.Result;
                        Assert.Equal(2, attachment.Etag);
                        Assert.Equal("File", attachment.Name);
                        Assert.Equal(size, stream.Position);
                        Assert.Equal(size, attachment.Size);
                        Assert.Equal("application/pdf", attachment.ContentType);
                        Assert.Equal(hash, attachment.Hash);
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
                    Assert.Equal(hash, attachment.Hash);

                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal((DocumentFlags.HasAttachments | DocumentFlags.FromReplication).ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    var attachmentMetadata = attachments.Single();
                    Assert.Equal("File", attachmentMetadata.GetString(nameof(AttachmentResult.Name)));
                    Assert.Equal("application/pdf", attachmentMetadata.GetString(nameof(AttachmentResult.ContentType)));
                    Assert.Equal(hash, attachmentMetadata.GetString(nameof(AttachmentResult.Hash)));
                    Assert.Equal(size, attachmentMetadata.GetNumber(nameof(AttachmentResult.Size)));
                }
            }
        }
    }
}