using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client.Attachments;
using FastTests.Server.Documents.Versioning;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Xunit;

namespace SlowTests.Client.Attachments
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
                    await VersioningHelper.SetupVersioning(Server.ServerStore, store1.Database, false, 4);
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
                    await VersioningHelper.SetupVersioning(Server.ServerStore, store2.Database);

                    for (var i = 0; i < 2; i++) // Make sure that we can import attachments twice and it will overwrite
                    {
                        await store2.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), file);

                        var stats = await store2.Admin.SendAsync(new GetStatisticsOperation());
                        Assert.Equal(1, stats.CountOfDocuments);
                        Assert.Equal(4, stats.CountOfRevisionDocuments);
                        Assert.Equal(14, stats.CountOfAttachments);
                        Assert.Equal(4, stats.CountOfUniqueAttachments);

                        using (var session = store2.OpenSession())
                        {
                            var readBuffer = new byte[1024 * 1024];
                            using (var attachmentStream = new MemoryStream(readBuffer))
                            using (var attachment = session.Advanced.GetAttachment("users/1", "big-file"))
                            {
                                attachment.Stream.CopyTo(attachmentStream);
                                Assert.Equal(2 + 20 * i, attachment.Details.Etag);
                                Assert.Equal("big-file", attachment.Details.Name);
                                Assert.Equal("zKHiLyLNRBZti9DYbzuqZ/EDWAFMgOXB+SwKvjPAINk=", attachment.Details.Hash);
                                Assert.Equal(999 * 1024, attachmentStream.Position);
                                Assert.Equal(Enumerable.Range(1, 999 * 1024).Select(x => (byte)x), readBuffer.Take((int)attachmentStream.Position));
                            }
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