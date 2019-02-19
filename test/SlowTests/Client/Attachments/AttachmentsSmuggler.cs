using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Attachments
{
    public class AttachmentsSmuggler : RavenTestBase
    {
        [Fact]
        public async Task ExportAndDeleteAttachmentThanCreateAnotherOneAndImport()
        {
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1"
                }))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "Fitzchak" }, "users/1");
                        session.SaveChanges();
                    }

                    using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                        store.Operations.Send(new PutAttachmentOperation("users/1", "file1", stream, "image/png"));

                    var exportOperation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    var exportResult = (SmugglerResult)exportOperation.WaitForCompletion();

                    Assert.Equal(1, exportResult.Documents.ReadCount);
                    Assert.Equal(1, exportResult.Documents.Attachments.ReadCount);

                    var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfAttachments);
                    Assert.Equal(1, stats.CountOfUniqueAttachments);

                    store.Operations.Send(new DeleteAttachmentOperation("users/1", "file1"));

                    stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfAttachments);
                    Assert.Equal(0, stats.CountOfUniqueAttachments);

                    using (var stream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                        store.Operations.Send(new PutAttachmentOperation("users/1", "file2", stream, "image/jpeg"));

                    stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfAttachments);
                    Assert.Equal(1, stats.CountOfUniqueAttachments);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    var importResult = (SmugglerResult)importOperation.WaitForCompletion();

                    Assert.Equal(1, importResult.Documents.ReadCount);
                    Assert.Equal(1, importResult.Documents.Attachments.ReadCount);

                    stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
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
                        Assert.Equal("file1", attachments[0].GetString(nameof(AttachmentName.Name)));
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].GetString(nameof(AttachmentName.Hash)));
                        Assert.Equal("image/png", attachments[0].GetString(nameof(AttachmentName.ContentType)));
                        Assert.Equal(3, attachments[0].GetLong(nameof(AttachmentName.Size)));

                        Assert.Equal("file2", attachments[1].GetString(nameof(Attachment.Name)));
                        Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", attachments[1].GetString(nameof(AttachmentName.Hash)));
                        Assert.Equal("image/jpeg", attachments[1].GetString(nameof(AttachmentName.ContentType)));
                        Assert.Equal(5, attachments[1].GetLong(nameof(AttachmentName.Size)));
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task ExportFullThanDeleteAttachmentAndCreateAnotherOneThanExportIncrementalThanImport()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_store1"
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Fitzchak"
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    store.Operations.Send(new PutAttachmentOperation("users/1", "file1", stream, "image/png"));

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };
                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                SpinWait.SpinUntil(() =>
                {
                    var getPeriodicBackupResult = store.Maintenance.Send(operation);
                    return getPeriodicBackupResult.Status?.LastEtag > 0;
                }, TimeSpan.FromSeconds(15));

                var etagForBackups = store.Maintenance.Send(operation).Status.LastEtag;
                store.Operations.Send(new DeleteAttachmentOperation("users/1", "file1"));
                using (var stream = new MemoryStream(new byte[] { 4, 5, 6 }))
                    store.Operations.Send(new PutAttachmentOperation("users/1", "file2", stream, "image/png"));

                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments);
                Assert.Equal(1, stats.CountOfAttachments);
                Assert.Equal(1, stats.CountOfUniqueAttachments);

                SpinWait.SpinUntil(() =>
                {
                    var newLastEtag = store.Maintenance.Send(operation).Status.LastEtag;
                    return newLastEtag != etagForBackups;
                }, TimeSpan.FromMinutes(2));
            }

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_store2"
            }))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), Directory.GetDirectories(backupPath).First());

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments);
                Assert.Equal(1, stats.CountOfAttachments);
                Assert.Equal(1, stats.CountOfUniqueAttachments);

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachment = metadata.GetObjects(Constants.Documents.Metadata.Attachments).Single();
                    Assert.Equal("file2", attachment.GetString(nameof(AttachmentName.Name)));
                }
            }
        }

        [Fact]
        public async Task ExportAndDeleteAttachmentAndImport()
        {
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1"
                }))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "Fitzchak" }, "users/1");
                        session.SaveChanges();
                    }

                    using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                        store.Operations.Send(new PutAttachmentOperation("users/1", "file1", stream, "image/png"));

                    var exportOperation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    var exportResult = (SmugglerResult)exportOperation.WaitForCompletion();

                    Assert.Equal(1, exportResult.Documents.ReadCount);
                    Assert.Equal(1, exportResult.Documents.Attachments.ReadCount);

                    var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfAttachments);
                    Assert.Equal(1, stats.CountOfUniqueAttachments);

                    store.Operations.Send(new DeleteAttachmentOperation("users/1", "file1"));

                    stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfAttachments);
                    Assert.Equal(0, stats.CountOfUniqueAttachments);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    var importResult = (SmugglerResult)importOperation.WaitForCompletion();

                    Assert.Equal(1, importResult.Documents.ReadCount);
                    Assert.Equal(1, importResult.Documents.Attachments.ReadCount);

                    stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
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
                        Assert.Equal("file1", attachment.GetString(nameof(AttachmentName.Name)));
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.GetString(nameof(AttachmentName.Hash)));
                        Assert.Equal("image/png", attachment.GetString(nameof(AttachmentName.ContentType)));
                        Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
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
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1"
                }))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "Fitzchak" }, "users/1");
                        session.SaveChanges();
                    }

                    var exportOperation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    var exportResult = (SmugglerResult)exportOperation.WaitForCompletion();

                    Assert.Equal(1, exportResult.Documents.ReadCount);
                    Assert.Equal(0, exportResult.Documents.Attachments.ReadCount);

                    var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfAttachments);
                    Assert.Equal(0, stats.CountOfUniqueAttachments);

                    using (var stream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                        store.Operations.Send(new PutAttachmentOperation("users/1", "file2", stream, "image/jpeg"));

                    stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfAttachments);
                    Assert.Equal(1, stats.CountOfUniqueAttachments);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    var importResult = (SmugglerResult)importOperation.WaitForCompletion();

                    Assert.Equal(1, importResult.Documents.ReadCount);
                    Assert.Equal(0, importResult.Documents.Attachments.ReadCount);

                    stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
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
                        Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", attachment.GetString(nameof(AttachmentName.Hash)));
                        Assert.Equal("image/jpeg", attachment.GetString(nameof(AttachmentName.ContentType)));
                        Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
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
            var file = GetTempFileName();
            try
            {
                var dbId2 = new Guid("99999999-48c4-421e-9466-999999999999");
                var dbId = new Guid("00000000-48c4-421e-9466-000000000000");

                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1"
                }))
                {
                    await SetDatabaseId(store1, dbId);

                    await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database, configuration =>
                    {
                        configuration.Collections["Users"].PurgeOnDelete = false;
                        configuration.Collections["Users"].MinimumRevisionsToKeep = 4;
                    });

                    using (var session = store1.OpenSession())
                    {
                        session.Store(new User { Name = "Fitzchak" }, "users/1");
                        session.SaveChanges();
                    }
                    using (var emptyStream = new MemoryStream(new byte[0]))
                    {
                        var result = store1.Operations.Send(new PutAttachmentOperation("users/1", "empty-file", emptyStream, "image/png"));
                        Assert.Equal("A:3", result.ChangeVector.Substring(0, 3));
                        Assert.Equal("empty-file", result.Name);
                        Assert.Equal("users/1", result.DocumentId);
                        Assert.Equal("image/png", result.ContentType);
                        Assert.Equal("DldRwCblQ7Loqy6wYJnaodHl30d3j3eH+qtFzfEv46g=", result.Hash);
                        Assert.Equal(0, result.Size);
                    }

                    var exportOperation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    var exportResult = (SmugglerResult)exportOperation.WaitForCompletion();

                    Assert.Equal(1, exportResult.Documents.ReadCount);
                    Assert.Equal(2, exportResult.RevisionDocuments.ReadCount);
                    Assert.Equal(1, exportResult.Documents.Attachments.ReadCount);
                    Assert.Equal(1, exportResult.RevisionDocuments.Attachments.ReadCount);

                    var stats = await store1.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(2, stats.CountOfRevisionDocuments);
                    Assert.Equal(2, stats.CountOfAttachments);
                    Assert.Equal(1, stats.CountOfUniqueAttachments);
                }

                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2"
                }))
                {
                    await SetDatabaseId(store2, dbId2);

                    var importOperation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    var importResult = (SmugglerResult)importOperation.WaitForCompletion();

                    Assert.Equal(1, importResult.Documents.ReadCount);
                    Assert.Equal(2, importResult.RevisionDocuments.ReadCount);
                    Assert.Equal(1, importResult.Documents.Attachments.ReadCount);
                    Assert.Equal(1, importResult.RevisionDocuments.Attachments.ReadCount);

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(3, stats.CountOfRevisionDocuments);
                    Assert.Equal(2 + 1, stats.CountOfAttachments); // the imported document will create 1 additional revision with 1 attachment
                    Assert.Equal(1, stats.CountOfUniqueAttachments);

                    using (var session = store2.OpenSession())
                    {
                        var readBuffer = new byte[1024 * 1024];
                        using (var attachmentStream = new MemoryStream(readBuffer))
                        using (var attachment = session.Advanced.Attachments.Get("users/1", "empty-file"))
                        {
                            attachment.Stream.CopyTo(attachmentStream);
                            Assert.Contains("A:1", attachment.Details.ChangeVector);
                            Assert.Equal("empty-file", attachment.Details.Name);
                            Assert.Equal(0, attachment.Details.Size);
                            Assert.Equal("DldRwCblQ7Loqy6wYJnaodHl30d3j3eH+qtFzfEv46g=", attachment.Details.Hash);
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

        [Fact]
        public async Task CanExportAndImportAttachmentsAndRevisionAttachments()
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1"
                }))
                {
                    await SetDatabaseId(store1, new Guid("00000000-48c4-421e-9466-000000000000"));
                    await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database, configuration =>
                    {
                        configuration.Collections["Users"].PurgeOnDelete = false;
                        configuration.Collections["Users"].MinimumRevisionsToKeep = 4;
                    });

                    AttachmentsRevisions.CreateDocumentWithAttachments(store1);
                    using (var bigStream = new MemoryStream(Enumerable.Range(1, 999 * 1024).Select(x => (byte)x).ToArray()))
                        store1.Operations.Send(new PutAttachmentOperation("users/1", "big-file", bigStream, "image/png"));

                    var exportOperation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    var exportResult = (SmugglerResult)exportOperation.WaitForCompletion();

                    Assert.Equal(1, exportResult.Documents.ReadCount);
                    Assert.Equal(4, exportResult.RevisionDocuments.ReadCount);
                    Assert.Equal(4, exportResult.Documents.Attachments.ReadCount);
                    Assert.Equal(10, exportResult.RevisionDocuments.Attachments.ReadCount);

                    var stats = await store1.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(4, stats.CountOfRevisionDocuments);
                    Assert.Equal(14, stats.CountOfAttachments);
                    Assert.Equal(4, stats.CountOfUniqueAttachments);
                }

                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2"
                }))
                {
                    var dbId = new Guid("00000000-48c4-421e-9466-000000000000");
                    await SetDatabaseId(store2, dbId);

                    await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database);

                    for (var i = 0; i < 2; i++) // Make sure that we can import attachments twice and it will overwrite
                    {
                        var importOperation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        var importResult = (SmugglerResult)importOperation.WaitForCompletion();

                        Assert.Equal(1, importResult.Documents.ReadCount);
                        Assert.Equal(4, importResult.RevisionDocuments.ReadCount);
                        Assert.Equal(4, importResult.Documents.Attachments.ReadCount);
                        Assert.Equal(4, importResult.RevisionDocuments.Attachments.ReadCount);

                        var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                        Assert.Equal(1, stats.CountOfDocuments);
                        Assert.Equal(5, stats.CountOfRevisionDocuments);
                        Assert.Equal(14 + 4, stats.CountOfAttachments); // the imported document will create 1 additional revision with 4 attachments
                        Assert.Equal(4, stats.CountOfUniqueAttachments);

                        using (var session = store2.OpenSession())
                        {
                            var readBuffer = new byte[1024 * 1024];
                            using (var attachmentStream = new MemoryStream(readBuffer))
                            using (var attachment = session.Advanced.Attachments.Get("users/1", "big-file"))
                            {
                                attachment.Stream.CopyTo(attachmentStream);
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
