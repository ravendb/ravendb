using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Documents.Versioning;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Xunit;

namespace FastTests.Client.Attachments
{
    public class AttachmentsSmuggler : RavenTestBase
    {
        [Fact]
        public async Task CanExportAndImportAttachmentsAndRevisionAttachments()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(dbSuffixIdentifier: "store1"))
                {
                    await VersioningHelper.SetupVersioning(Server.ServerStore, store1.DefaultDatabase, false, 4);
                    AttachmentsVersioning.CreateDocumentWithAttachments(store1);
                    using (var bigStream = new MemoryStream(Enumerable.Range(1, 999 * 1024).Select(x => (byte)x).ToArray()))
                        store1.Operations.Send(new PutAttachmentOperation("users/1", "big-file", bigStream, "image/png"));

                    /*var result = */await store1.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), file);
                    // TODO: RavenDB-6936 store.Smuggler.Export and Import method should return the SmugglerResult

                    var stats = await store1.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(4, stats.CountOfRevisionDocuments);
                    Assert.Equal(14, stats.CountOfAttachments);
                    Assert.Equal(4, stats.CountOfUniqueAttachments);
                }

                using (var store2 = GetDocumentStore(dbSuffixIdentifier: "store2"))
                {
                    await VersioningHelper.SetupVersioning(Server.ServerStore, store2.DefaultDatabase);

                    await store2.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), file);

                    var stats = await store2.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(5, stats.CountOfRevisionDocuments);
                    Assert.Equal(14, stats.CountOfAttachments);
                    Assert.Equal(4, stats.CountOfUniqueAttachments);

                    using (var session = store2.OpenSession())
                    {
                        var readBuffer = new byte[1024 * 1024];
                        using (var attachmentStream = new MemoryStream(readBuffer))
                        {
                            var attachment = session.Advanced.GetAttachment("users/1", "big-file", (result, stream) => stream.CopyTo(attachmentStream));
                            Assert.Equal(2, attachment.Etag);
                            Assert.Equal("big-file", attachment.Name);
                            Assert.Equal("OLSEi3K4Iio9JV3ymWJeF12Nlkjakwer", attachment.Hash);
                            Assert.Equal(999 * 1024, attachmentStream.Position);
                            Assert.Equal(Enumerable.Range(1, 999 * 1024).Select(x => (byte)x), readBuffer.Take((int)attachmentStream.Position));
                        }
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task ExportAndDeleteAttachmentThanCreateAnotherOneAndImport()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store = GetDocumentStore(dbSuffixIdentifier: "store1"))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "Fitzchak" }, "users/1");
                        session.SaveChanges();
                    }

                    using (var stream = new MemoryStream(new byte[] {1, 2, 3}))
                        store.Operations.Send(new PutAttachmentOperation("users/1", "file1", stream, "image/png"));

                    await store.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), file);

                    var stats = await store.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfAttachments);
                    Assert.Equal(1, stats.CountOfUniqueAttachments);

                    store.Operations.Send(new DeleteAttachmentOperation("users/1", "file1"));

                    stats = await store.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfAttachments);
                    Assert.Equal(0, stats.CountOfUniqueAttachments);

                    using (var stream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                        store.Operations.Send(new PutAttachmentOperation("users/1", "file2", stream, "image/jpeg"));

                    stats = await store.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfAttachments);
                    Assert.Equal(1, stats.CountOfUniqueAttachments);

                    await store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), file);

                    stats = await store.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(2, stats.CountOfAttachments);
                    Assert.Equal(2, stats.CountOfUniqueAttachments);

                    using (var session = store.OpenSession())
                    {
                        var user = session.Load<User>("users/1");

                        var metadata = session.Advanced.GetMetadataFor(user);
                        Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                        var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                        Assert.Equal(2, attachments.Length);
                        Assert.Equal("file1", attachments[0].GetString(nameof(AttachmentResult.Name)));
                        Assert.Equal("JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", attachments[0].GetString(nameof(AttachmentResult.Hash)));
                        Assert.Equal("image/png", attachments[0].GetString(nameof(AttachmentResult.ContentType)));
                        Assert.Equal(3, attachments[0].GetNumber(nameof(AttachmentResult.Size)));

                        Assert.Equal("file2", attachments[1].GetString(nameof(Attachment.Name)));
                        Assert.Equal("PN5EZXRY470m7BLxu9MsOi/WwIRIq4WN", attachments[1].GetString(nameof(AttachmentResult.Hash)));
                        Assert.Equal("image/jpeg", attachments[1].GetString(nameof(AttachmentResult.ContentType)));
                        Assert.Equal(5, attachments[1].GetNumber(nameof(AttachmentResult.Size)));
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task ExportAndDeleteAttachmentAndImport()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store = GetDocumentStore(dbSuffixIdentifier: "store1"))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "Fitzchak" }, "users/1");
                        session.SaveChanges();
                    }

                    using (var stream = new MemoryStream(new byte[] {1, 2, 3}))
                        store.Operations.Send(new PutAttachmentOperation("users/1", "file1", stream, "image/png"));

                    await store.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), file);

                    var stats = await store.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfAttachments);
                    Assert.Equal(1, stats.CountOfUniqueAttachments);

                    store.Operations.Send(new DeleteAttachmentOperation("users/1", "file1"));

                    stats = await store.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfAttachments);
                    Assert.Equal(0, stats.CountOfUniqueAttachments);

                    await store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), file);

                    stats = await store.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfAttachments);
                    Assert.Equal(1, stats.CountOfUniqueAttachments);

                    using (var session = store.OpenSession())
                    {
                        var user = session.Load<User>("users/1");

                        var metadata = session.Advanced.GetMetadataFor(user);
                        Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                        var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                        var attachment = attachments.Single();
                        Assert.Equal("file1", attachment.GetString(nameof(AttachmentResult.Name)));
                        Assert.Equal("JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", attachment.GetString(nameof(AttachmentResult.Hash)));
                        Assert.Equal("image/png", attachment.GetString(nameof(AttachmentResult.ContentType)));
                        Assert.Equal(3, attachment.GetNumber(nameof(AttachmentResult.Size)));
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task ExportWithoutAttachmentAndCreateOneAndImport()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store = GetDocumentStore(dbSuffixIdentifier: "store1"))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "Fitzchak" }, "users/1");
                        session.SaveChanges();
                    }

                    await store.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), file);

                    var stats = await store.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfAttachments);
                    Assert.Equal(0, stats.CountOfUniqueAttachments);

                    using (var stream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                        store.Operations.Send(new PutAttachmentOperation("users/1", "file2", stream, "image/jpeg"));

                    stats = await store.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfAttachments);
                    Assert.Equal(1, stats.CountOfUniqueAttachments);

                    await store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), file);

                    stats = await store.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfAttachments);
                    Assert.Equal(1, stats.CountOfUniqueAttachments);

                    using (var session = store.OpenSession())
                    {
                        var user = session.Load<User>("users/1");

                        var metadata = session.Advanced.GetMetadataFor(user);
                        Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                        var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                        var attachment = attachments.Single();
                        Assert.Equal("file2", attachment.GetString(nameof(Attachment.Name)));
                        Assert.Equal("PN5EZXRY470m7BLxu9MsOi/WwIRIq4WN", attachment.GetString(nameof(AttachmentResult.Hash)));
                        Assert.Equal("image/jpeg", attachment.GetString(nameof(AttachmentResult.ContentType)));
                        Assert.Equal(5, attachment.GetNumber(nameof(AttachmentResult.Size)));
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task ExportEmptyStream()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(dbSuffixIdentifier: "store1"))
                {
                    await VersioningHelper.SetupVersioning(Server.ServerStore, store1.DefaultDatabase, false, 4);
                    using (var session = store1.OpenSession())
                    {
                        session.Store(new User { Name = "Fitzchak" }, "users/1");
                        session.SaveChanges();
                    }
                    using (var emptyStream = new MemoryStream(new byte[0]))
                    {
                        var result = store1.Operations.Send(new PutAttachmentOperation("users/1", "empty-file", emptyStream, "image/png"));
                        Assert.Equal(3, result.Etag);
                        Assert.Equal("empty-file", result.Name);
                        Assert.Equal("users/1", result.DocumentId);
                        Assert.Equal("image/png", result.ContentType);
                        Assert.Equal("y9FBPcrzBQC2X8aERrEGRpnp2FE320bv", result.Hash);
                        Assert.Equal(0, result.Size);
                    }

                    await store1.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), file);

                    var stats = await store1.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(2, stats.CountOfRevisionDocuments);
                    Assert.Equal(2, stats.CountOfAttachments);
                    Assert.Equal(1, stats.CountOfUniqueAttachments);
                }

                using (var store2 = GetDocumentStore(dbSuffixIdentifier: "store2"))
                {
                    await VersioningHelper.SetupVersioning(Server.ServerStore, store2.DefaultDatabase);

                    await store2.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), file);

                    var stats = await store2.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(3, stats.CountOfRevisionDocuments);
                    Assert.Equal(2, stats.CountOfAttachments);
                    Assert.Equal(1, stats.CountOfUniqueAttachments);

                    using (var session = store2.OpenSession())
                    {
                        var readBuffer = new byte[1024 * 1024];
                        using (var attachmentStream = new MemoryStream(readBuffer))
                        {
                            var attachment = session.Advanced.GetAttachment("users/1", "empty-file", (result, stream) => stream.CopyTo(attachmentStream));
                            Assert.Equal(1, attachment.Etag);
                            Assert.Equal("empty-file", attachment.Name);
                            Assert.Equal(0, attachment.Size);
                            Assert.Equal("y9FBPcrzBQC2X8aERrEGRpnp2FE320bv", attachment.Hash);
                            Assert.Equal(0, attachmentStream.Position);
                            Assert.Equal(new byte[0], readBuffer.Take((int)attachmentStream.Position));
                        }
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }
    }
}