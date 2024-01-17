using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20843 : ReplicationTestBase
    {
        public RavenDB_20843(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Attachments | RavenTestCategory.BackupExportImport)]
        public async Task AttachmentsWithSameNameShouldNotExistTwiceAfterImportFrom2Stores()
        {
            var file1 = GetTempFileName();
            var file2 = GetTempFileName();

            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenSession())
                    {
                        var user1 = new User { Name = "EGR" };
                        session.Store(user1, "users/1");
                        session.SaveChanges();
                    }

                    using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        var result = store1.Operations.Send(new PutAttachmentOperation("users/1", "foo/bar", profileStream, "image/png"));
                        Assert.Equal("foo/bar", result.Name);
                        Assert.Equal("users/1", result.DocumentId);
                        Assert.Equal("image/png", result.ContentType);
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                    }

                    using (var session = store2.OpenSession())
                    {
                        var user1 = new User { Name = "EGR" };
                        session.Store(user1, "users/1");
                        session.SaveChanges();
                    }

                    using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                    {
                        var result = store2.Operations.Send(new PutAttachmentOperation("users/1", "foo/bar", backgroundStream, "image/png"));
                        Assert.Equal("foo/bar", result.Name);
                        Assert.Equal("users/1", result.DocumentId);
                        Assert.Equal("image/png", result.ContentType);
                        Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", result.Hash);
                    }

                    var exportOperation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file1);
                    var exportResult = (SmugglerResult)await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    Assert.Equal(1, exportResult.Documents.ReadCount);
                    Assert.Equal(1, exportResult.Documents.Attachments.ReadCount);

                    exportOperation = await store2.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file2);
                    exportResult = (SmugglerResult)await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    Assert.Equal(1, exportResult.Documents.ReadCount);
                    Assert.Equal(1, exportResult.Documents.Attachments.ReadCount);

                    using (var store3 = GetDocumentStore())
                    {
                        var importOperation = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file1);
                        var importResult = (SmugglerResult)await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                        Assert.Equal(1, importResult.Documents.ReadCount);
                        Assert.Equal(1, importResult.Documents.Attachments.ReadCount);

                        importOperation = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file2);
                        importResult = (SmugglerResult)await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    
                        Assert.Equal(1, importResult.Documents.ReadCount);
                        Assert.Equal(1, importResult.Documents.Attachments.ReadCount);

                        var database3 = await GetDatabase(store3.Database);
                        using (database3.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            using (DocumentIdWorker.GetSliceFromId(context, "users/1", out Slice docIdSlice))
                            {
                                var attachments = database3.DocumentsStorage.AttachmentsStorage.GetAttachmentsForDocument(context, docIdSlice).ToList();
                                Assert.NotNull(attachments);
                                Assert.Equal(1, attachments.Count); // we should have only one attachment here 
                                Assert.Equal("foo/bar", attachments[0].Name);
                                Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", attachments[0].Base64Hash.ToString());
                            }
                        }
                    }
                }
            }
            finally
            {
                File.Delete(file1);
                File.Delete(file2);
            }
        }
    }
}
