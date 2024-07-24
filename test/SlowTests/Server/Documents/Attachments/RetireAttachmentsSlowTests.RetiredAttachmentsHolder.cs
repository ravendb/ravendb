using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Documents.Replication;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Raven.Server.ServerWide.Context;
using SlowTests.Client.Attachments;
using SlowTests.Server.Documents.ETL.Olap;
using Xunit;

namespace SlowTests.Server.Documents.Attachments;

public partial class RetireAttachmentsSlowTests
{
    public class RetiredAttachmentsHolder : IAsyncDisposable
    {
        private readonly RavenTestBase _parent;
        public readonly S3Settings Settings;
        public readonly List<MyAttachment> Attachments;


        public RetiredAttachmentsHolder(RavenTestBase parent, [CallerMemberName] string caller = null)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));

            Settings = _parent.Etl.GetS3Settings($"{caller}-{Guid.NewGuid()}", string.Empty);
            Assert.NotNull(Settings);
            Attachments = new List<MyAttachment>();
        }

        public static async Task<List<S3FileInfoDetails>> GetBlobsFromS3AndAssertForCount(S3Settings settings, int expected, int timeout = 120_000)
        {
            List<S3FileInfoDetails> cloudObjects = null;
            var val3 = await WaitForValueAsync(async () =>
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
                using (var s3Client = new RavenAwsS3Client(settings, EtlTestBase.DefaultBackupConfiguration, cancellationToken: cts.Token))
                {
                    var prefix = $"{settings.RemoteFolderName}";
                    cloudObjects = await s3Client.ListAllObjectsAsync(prefix, string.Empty, false);
                    return cloudObjects.Count;
                }
            }, expected, timeout);
            Assert.Equal(expected, val3);

            if (expected == 0)
                Assert.Empty(cloudObjects);
            else
                Assert.NotNull(cloudObjects);

            return cloudObjects;
        }

        public static async Task PutRetireAttachmentsConfiguration(DocumentStore store, S3Settings settings, List<string> collections = null, string database = null)
        {
            if (collections == null)
                collections = new List<string> { "Orders" };
            if(string.IsNullOrEmpty(database))
                database = store.Database;
            await store.Maintenance.ForDatabase(database).SendAsync(new ConfigureRetireAttachmentsOperation(new RetireAttachmentsConfiguration()
            {
                S3Settings = settings,
                Disabled = false,

                RetirePeriods = collections.ToDictionary(x => x, x => TimeSpan.FromMinutes(3)),

                RetireFrequencyInSec = 1000
            }));
        }
        public async Task DeleteObjects(S3Settings s3Settings)
        {
            if (s3Settings == null)
                return;

            await S3Tests.DeleteObjects(s3Settings, prefix: $"{s3Settings.RemoteFolderName}", delimiter: string.Empty);
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var attachment in Attachments)
            {
                if (attachment.Stream == null)
                    continue;

                try
                {
                    await attachment.Stream.DisposeAsync();
                }
                catch
                {
                    // ignored
                }
            }

            await DeleteObjects(Settings);
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
            Assert.Equal(collections.Count, ids.GroupBy(x=>x.Item2).Count());
        }

        public async Task PopulateDocsWithRandomAttachments(DocumentStore store, int size, List<(string Id, string Collection)> ids, int attachmentsPerDoc, int start = 0)
        {
            // put attachments
            foreach (var (id, collection) in ids)
            {
                for (int i = 0; i < attachmentsPerDoc; i++)
                {
                    var rnd = new Random();
                    var b = new byte[size];
                    rnd.NextBytes(b);

                    var profileStream = new MemoryStream(b);
                    var name = $"test_{i + start}.png";
                    await store.Operations.SendAsync(new PutAttachmentOperation(id, name, profileStream, "image/png"));

                    profileStream.Position = 0;
                    Attachments.Add(new RetiredAttachmentsHolder.MyAttachment()
                    {
                        Name = name,
                        DocumentId = id,
                        Collection = collection,
                        Stream = profileStream,
                        ContentType = "image/png"
                    });
                }
            }
        }

        // add attachments storage key, RetiredKey, and hash to holder
        public void GetStorageAttachmentsMetadataFromAllAttachments(DocumentDatabase database)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            using (var _documentInfoHelper = new DocumentInfoHelper(context))

            {
                foreach (var attachment in database.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context))
                {
                    using (var docId = _documentInfoHelper.GetDocumentId(attachment.Key))
                    {
                        var t = Attachments.FirstOrDefault(x => x.DocumentId.ToLowerInvariant() == docId && x.Name == attachment.Name);
                        Assert.NotNull(t);
                        Attachments.Remove(t);
                        t.Key = attachment.Key;
                        t.Hash = attachment.Base64Hash.ToString();
                        t.RetireAt = attachment.RetiredAt;
                        //TODO: egor I can use getcollecton method here
                        t.RetiredKey =
                            $"{Settings.RemoteFolderName}/{t.Collection}/{Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(attachment.Key))}";
                          //  $"{Settings.RemoteFolderName}/{database.Name}/{t.Collection}/{Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(attachment.Key))}";
                        Attachments.Add(t);
                    }
                }

                Attachments.GroupBy(x=>x.RetiredKey).ToList().ForEach(x =>
                {
                    Assert.Single(x);
                });
            }
        }

        public async Task AssertAllRetiredAttachments(IDocumentStore store, List<S3FileInfoDetails> cloudObjects, int size)
        {
            foreach (var attachment in Attachments)
            {
                Assert.Contains(cloudObjects, x => x.FullPath.Contains(attachment.RetiredKey));

                attachment.Stream.Position = 0;
                await GetAndCompareRetiredAttachment(store, attachment.DocumentId, attachment.Name, attachment.Hash, attachment.ContentType, attachment.Stream, size);
            }
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

        public class MyAttachment : AttachmentDetails
        {
            public string Key { get; set; }
            public MemoryStream Stream { get; set; }
            public string RetiredKey { get; set; }
            public string Collection { get; set; }
        }
    }
}
