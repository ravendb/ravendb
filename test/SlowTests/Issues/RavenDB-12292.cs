using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12292 : RavenTestBase
    {
        public RavenDB_12292(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanExportRavenDbWithoutAttachments()
        {
            var folder = NewDataPath(forceCreateDir: true);
            var file = Path.Combine(folder, "export.ravendbdump");

            using (var store1 = GetDocumentStore())
            {
                using (var session = store1.OpenSession())
                {
                    var user = new User
                    {
                        Name = "John"
                    };

                    session.Store(user, "users/1");

                    var buffer = new byte[1024 * 1024];
                    new Random(1).NextBytes(buffer);

                    session.Advanced.Attachments.Store(user, "photo.jpg", new MemoryStream(buffer));

                    session.SaveChanges();
                }

                using (var store2 = GetDocumentStore())
                {
                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                    }, file);

                    operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                    var fileInfo = new FileInfo(file);
                    Assert.True(fileInfo.Exists);
                    Assert.True(file.Length < 500);

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                    using (var session = store2.OpenSession())
                    {
                        var user = session.Load<User>("users/1");
                        Assert.NotNull(user);
                        Assert.Equal("John", user.Name);

                        var attachments = session.Advanced.Attachments.GetNames(user);
                        Assert.Empty(attachments);

                        using (var photo = session.Advanced.Attachments.Get(user, "photo.jpg"))
                        {
                            Assert.Null(photo);
                        }

                        var metadata = session.Advanced.GetMetadataFor(user);
                        Assert.False(metadata.TryGetValue(Constants.Documents.Metadata.Attachments, out _));
                    }
                }
            }
        }

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
                var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                operation.WaitForCompletion(TimeSpan.FromSeconds(15));
            }
            // all data import
            using (var store2 = GetDocumentStore())
            {
                var operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

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

                var operation = await store3.Smuggler.ImportAsync(importOptions, file);
                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

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

                var operation = await store4.Smuggler.ImportAsync(importOptions, file);
                operation.WaitForCompletion(TimeSpan.FromSeconds(15));
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
