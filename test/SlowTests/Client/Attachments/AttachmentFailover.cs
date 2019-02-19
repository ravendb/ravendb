using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client.Attachments
{
    public class AttachmentFailover : ClusterTestBase
    {
        [Fact]
        public async Task PutAttachmentsWithFailover_Session()
        {
            const int size = 512 * 1024;
            const string hash = "BfKA8g/BJuHOTHYJ+A6sOt9jmFSVEDzCM3EcLLKCRMU=";
            UseNewLocalServer();
            var leader = await CreateRaftClusterAndGetLeader(3);
            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ModifyDatabaseRecord = record =>
                {
                    record.Topology = new DatabaseTopology
                    {
                        DynamicNodesDistribution = false,
                        Members =
                        {
                            leader.ServerStore.NodeTag,
                            Servers.First(x=>x!= leader).ServerStore.NodeTag
                        }
                    };
                }
            }))
            {
                using (var session = (DocumentSession)store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Fitzchak"
                    }, "users/1");
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
                    session.Advanced.Attachments.Store("users/1", "File", stream, "application/pdf");

                    // SaveSession with failover
                    var saveChangesOperation = new BatchOperation(session);
                    using (var command = saveChangesOperation.CreateRequest())
                    {
                        var (currentIndex, currentNode) = await session.RequestExecutor.GetPreferredNode();
                        Assert.Equal(currentNode.ClusterTag, leader.ServerStore.NodeTag);
                        var currentServer = Servers.Single(x => x.ServerStore.NodeTag == currentNode.ClusterTag);
                        stream.Position++; // simulating that we already started to call this and we need to reset
                        DisposeServerAndWaitForFinishOfDisposal(currentServer);
                        var task = session.RequestExecutor.ExecuteAsync(currentNode, currentIndex, session.Context, command);
                        await task;
                        saveChangesOperation.SetResult(command.Result);
                    }
                }
                using (var session = store.OpenSession())
                using (var dummyStream = new BigDummyStream(size))
                using (var attachment = session.Advanced.Attachments.Get("users/1", "File"))
                {
                    attachment.Stream.CopyTo(dummyStream);
                    Assert.Equal("File", attachment.Details.Name);
                    Assert.Equal(size, dummyStream.Position);
                    Assert.Equal(size, attachment.Details.Size);
                    Assert.Equal("application/pdf", attachment.Details.ContentType);
                    Assert.Equal(hash, attachment.Details.Hash);

                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Contains(DocumentFlags.HasAttachments.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    var attachmentMetadata = attachments.Single();
                    Assert.Equal("File", attachmentMetadata.GetString(nameof(AttachmentName.Name)));
                    Assert.Equal("application/pdf", attachmentMetadata.GetString(nameof(AttachmentName.ContentType)));
                    Assert.Equal(hash, attachmentMetadata.GetString(nameof(AttachmentName.Hash)));
                    Assert.Equal(size, attachmentMetadata.GetLong(nameof(AttachmentName.Size)));
                }
            }
        }

        [Fact]
        public async Task PutAttachmentsWithFailover_LowLevel()
        {
            const string hash = "BfKA8g/BJuHOTHYJ+A6sOt9jmFSVEDzCM3EcLLKCRMU=";
            const int size = 512 * 1024;
            UseNewLocalServer();
            var leader = await CreateRaftClusterAndGetLeader(3);
            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ModifyDatabaseRecord = record =>
                {
                    record.Topology = new DatabaseTopology
                    {
                        DynamicNodesDistribution = false,
                        Members =
                        {
                            leader.ServerStore.NodeTag,
                            Servers.First(x=>x!= leader).ServerStore.NodeTag
                        }
                    };
                }
            }))
            {
                using (var session = (DocumentSession)store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Fitzchak"
                    }, "users/1");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        session,
                        "users/1",
                        u => u.Name.Equals("Fitzchak"),
                        TimeSpan.FromSeconds(10)));
                }

                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var stream = new BigDummyStream(size))
                {
                    var command = new PutAttachmentOperation("users/1", "File", stream, "application/pdf")
                        .GetCommand(store, store.Conventions, context, requestExecutor.Cache);

                    var (currentIndex, currentNode) = await requestExecutor.GetPreferredNode();
                    var currentServer = Servers.Single(x => x.ServerStore.NodeTag == currentNode.ClusterTag);
                    Assert.Equal(currentNode.ClusterTag, leader.ServerStore.NodeTag);

                    stream.Position++; // simulating that we already started to call this and we need to reset
                    DisposeServerAndWaitForFinishOfDisposal(currentServer);
                    var task = requestExecutor.ExecuteAsync(currentNode, currentIndex, context, command);

                    await task;
                    var attachment = command.Result;
                    Assert.Equal("File", attachment.Name);
                    Assert.Equal(size, stream.Position);
                    Assert.Equal(size, attachment.Size);
                    Assert.Equal("application/pdf", attachment.ContentType);
                    Assert.Equal(hash, attachment.Hash);
                }

                using (var session = store.OpenSession())
                using (var dummyStream = new BigDummyStream(size))
                using (var attachment = session.Advanced.Attachments.Get("users/1", "File"))
                {
                    attachment.Stream.CopyTo(dummyStream);
                    Assert.Equal("File", attachment.Details.Name);
                    Assert.Equal(size, dummyStream.Position);
                    Assert.Equal(size, attachment.Details.Size);
                    Assert.Equal("application/pdf", attachment.Details.ContentType);
                    Assert.Equal(hash, attachment.Details.Hash);

                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Contains(DocumentFlags.HasAttachments.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    var attachmentMetadata = attachments.Single();
                    Assert.Equal("File", attachmentMetadata.GetString(nameof(AttachmentName.Name)));
                    Assert.Equal("application/pdf", attachmentMetadata.GetString(nameof(AttachmentName.ContentType)));
                    Assert.Equal(hash, attachmentMetadata.GetString(nameof(AttachmentName.Hash)));
                    Assert.Equal(size, attachmentMetadata.GetLong(nameof(AttachmentName.Size)));
                }
            }
        }
    }
}
