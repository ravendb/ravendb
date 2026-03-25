using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Extensions;
using SlowTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Attachments;

public abstract class RemoteAttachmentsHolderBase : ReplicationTestBase
{
    protected readonly List<RemoteAttachment> Attachments;

    protected RemoteAttachmentsHolderBase(ITestOutputHelper output) : base(output)
    {
        Attachments = new List<RemoteAttachment>();
    }

    public static async Task CreateDocs(DocumentStore store, int docsCount, List<(string, string)> ids, List<string> collections = null)
    {
        if (collections == null)
            collections = new List<string> { "Orders" };

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < docsCount; i++)
            {
                var collection = collections[i % collections.Count];
                switch (collection)
                {
                    case "Orders":
                        var id = $"Orders/{i}";
                        await session.StoreAsync(new Order
                        {
                            Id = id,
                            OrderedAt = new DateTime(2024, 1, 1),
                            ShipVia = $"Shippers/2",
                            Company = $"Companies/2"
                        });

                        ids.Add((id, collection));
                        break;
                    case "Products":
                        id = $"Products/{i}";
                        await session.StoreAsync(new Product
                        {
                            Id = id,
                            Discontinued = false
                        });

                        ids.Add((id, collection));
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

            }
            await session.SaveChangesAsync();
        }

        Assert.Equal(docsCount, ids.Count);
        Assert.Equal(collections.Count, ids.GroupBy(x => x.Item2).Count());
    }

    public static async Task GetAndCompareRemoteAttachment(IDocumentStore store, string id, string attachmentName, string hash, string contentType, MemoryStream stream, int streamSize, string identifier, RemoteAttachmentFlags flags = RemoteAttachmentFlags.Remote)
    {
        var remote = await store.Operations.SendAsync(new GetAttachmentOperation(id, attachmentName, AttachmentType.Document, null));
        await CompareAttachment(attachmentName, hash, contentType, stream, streamSize, identifier, flags, remote.Details, remote.Stream);
    }

    internal static async Task CompareAttachment(string attachmentName, string hash, string contentType, MemoryStream stream, long streamSize, string identifier, RemoteAttachmentFlags flags,
        AttachmentDetails remote, Stream stream1)
    {
        Assert.NotNull(remote);
        Assert.Equal(hash, remote.Hash);
        Assert.Equal(contentType, remote.ContentType);
        Assert.Equal(attachmentName, remote.Name);
        Assert.Equal(streamSize, remote.Size);

        if (flags == RemoteAttachmentFlags.None)
        {
            Assert.True(remote.RemoteParameters.IsLocalStorageAttachment());
        }
        else
        {
            Assert.NotNull(remote.RemoteParameters);
            Assert.Equal(flags, remote.RemoteParameters.Flags);
            Assert.NotNull(remote.RemoteParameters.At);
            Assert.Equal(identifier, remote.RemoteParameters.Identifier);
        }

        using var remoteStream = new MemoryStream();
        await stream1.CopyToAsync(remoteStream);
        stream.Position = 0;
        remoteStream.Position = 0;
        await AttachmentsStreamTests.CompareStreamsAsync(stream, remoteStream);
    }

    public static int GetDocsAndAttachmentCount(int attachmentsCount, out int attachmentsPerDoc)
    {
        var docsCount = attachmentsCount <= 32 ? 1 : attachmentsCount / 32;
        attachmentsPerDoc = attachmentsCount / docsCount;
        return docsCount;
    }

    protected class RemoteAttachment : AttachmentDetails
    {
        public string Key { get; set; }
        public MemoryStream Stream { get; set; }
        public string RemoteKey { get; set; }
    }

    public class FileInfoDetails
    {
        public string FullPath { get; set; }

        public DateTime LastModified { get; set; }
    }
}
