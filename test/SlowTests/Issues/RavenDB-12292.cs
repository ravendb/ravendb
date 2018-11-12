using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    class RavenDB_12292 : RavenTestBase
    {
        [Fact]
        public async Task CanImportRavenDbWithoutAttachments()
        {
            var folder = NewDataPath(forceCreateDir: true);
            var file = Path.Combine(folder, "export.ravendbdump");
            var id = "users/1";

            using (var store1 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Egor"
                    }, id);

                    await session.SaveChangesAsync();
                }
                for (int i = 0; i < 10; i++)
                {
                    var attachmentFile = Path.Combine(folder, $"attachment.{i}");
                    // create file
                    using (FileStream fs = File.Create(attachmentFile))
                    {
                        var info = new UTF8Encoding(true).GetBytes($"Hi I am attachment.{i} file!");
                        fs.Write(info, 0, info.Length);
                    }
                    // add attachment
                    using (Stream sr = File.OpenRead(attachmentFile))
                    {
                        store1.Operations.Send(
                            new PutAttachmentOperation(id,
                                $"attachment.{i}",
                                sr));
                    }
                }
                using (var session = store1.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        Assert.True(await session.Advanced.Attachments.ExistsAsync(id, $"attachment.{i}"));
                    }
                    await session.SaveChangesAsync();
                }
                await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
            }
            // all data import
            using (var store2 = GetDocumentStore())
            {
                await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                using (var session = store2.OpenAsyncSession())
                {
                    var metadata = session.Advanced.GetMetadataFor(await session.LoadAsync<User>(id));
                    Assert.True(metadata.ContainsKey("@attachments"));
                    Assert.Equal("HasRevisions, HasAttachments", metadata.GetString("@flags"));
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(12, revisionsMetadata.Count); // +1 revision added when importing
                    for (int i = 0; i < 10; i++)
                    {
                        Assert.True(await session.Advanced.Attachments.ExistsAsync(id, $"attachment.{i}"));
                    }
                }
            }

            // no Attachments
            using (var store3 = GetDocumentStore())
            {
                var importOptions = new DatabaseSmugglerImportOptions();
                importOptions.OperateOnTypes -= DatabaseItemType.Attachments;

                await store3.Smuggler.ImportAsync(importOptions, file);
                using (var session = store3.OpenAsyncSession())
                {
                    var metadata = session.Advanced.GetMetadataFor(await session.LoadAsync<User>(id));
                    Assert.False(metadata.ContainsKey("@attachments"));
                    Assert.Equal("HasRevisions", metadata.GetString("@flags"));
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(12, revisionsMetadata.Count);
                }
            }
            // if doc had attachments => they must be kept.
            using (var store4 = GetDocumentStore())
            {
                using (var session = store4.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Egor"
                    }, id);
                    await session.SaveChangesAsync();

                    var attachmentFile = Path.Combine(folder, "attachment.kept");
                    // create file
                    using (FileStream fs = File.Create(attachmentFile))
                    {
                        var info = new UTF8Encoding(true).GetBytes("Hi I am attachment.kept file!");
                        fs.Write(info, 0, info.Length);
                    }
                    // add attachment
                    using (Stream sr = File.OpenRead(attachmentFile))
                    {
                        store4.Operations.Send(
                            new PutAttachmentOperation(id,
                                "attachment.kept",
                                sr));
                    }
                }
                var importOptions = new DatabaseSmugglerImportOptions();
                importOptions.OperateOnTypes -= DatabaseItemType.Attachments;

                await store4.Smuggler.ImportAsync(importOptions, file);
                using (var session = store4.OpenAsyncSession())
                {
                    var metadata = session.Advanced.GetMetadataFor(await session.LoadAsync<User>(id));
                    Assert.True(metadata.ContainsKey("@attachments"));
                    Assert.True(await session.Advanced.Attachments.ExistsAsync(id, "attachment.kept"));
                    Assert.Equal("HasRevisions, HasAttachments", metadata.GetString("@flags"));
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(11, revisionsMetadata.Count);
                }
            }
        }
    }
}
