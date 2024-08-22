using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using SlowTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments;

public abstract class RetiredAttachmentsHolderBase : ReplicationTestBase
{
    protected readonly List<RetiredAttachment> Attachments;

    protected RetiredAttachmentsHolderBase(ITestOutputHelper output) : base(output)
    {
        Attachments = new List<RetiredAttachment>();
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

    public static async Task GetAndCompareRetiredAttachment(IDocumentStore store, string id, string attachmentName, string hash, string contentType, MemoryStream stream, int streamSize)
    {
        var retired = await store.Operations.SendAsync(new GetRetiredAttachmentOperation(id, attachmentName));
        Assert.NotNull(retired);
        Assert.Equal(hash, retired.Details.Hash);
        Assert.Equal(contentType, retired.Details.ContentType);
        Assert.Equal(attachmentName, retired.Details.Name);
        Assert.Equal(streamSize, retired.Details.Size);
        Assert.Equal(AttachmentFlags.Retired, retired.Details.Flags);
        Assert.Null(retired.Details.RetireAt);
        using var retiredStream = new MemoryStream();
        await retired.Stream.CopyToAsync(retiredStream);
        stream.Position = 0;
        retiredStream.Position = 0;
        await AttachmentsStreamTests.CompareStreamsAsync(stream, retiredStream);
    }

    public static int GetDocsAndAttachmentCount(int attachmentsCount, out int attachmentsPerDoc)
    {
        var docsCount = attachmentsCount <= 32 ? 1 : attachmentsCount / 32;
        attachmentsPerDoc = attachmentsCount / docsCount;
        return docsCount;
    }

    protected class RetiredAttachment : AttachmentDetails
    {
        public string Key { get; set; }
        public MemoryStream Stream { get; set; }
        public string RetiredKey { get; set; }
        public string Collection { get; set; }
    }

    public class FileInfoDetails
    {
        public string FullPath { get; set; }

        public DateTime LastModified { get; set; }
    }
}
