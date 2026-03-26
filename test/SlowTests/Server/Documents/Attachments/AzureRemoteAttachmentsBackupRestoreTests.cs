using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Extensions;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Attachments
{
    public class AzureRemoteAttachmentsBackupRestoreTests : RemoteAttachmentsAzureBase
    {
        public AzureRemoteAttachmentsBackupRestoreTests(ITestOutputHelper output) : base(output)
        {
        }

        [AzureRetryTheory]
        [InlineData(1, 3, BackupType.Backup)]
        [InlineData(64, 3, BackupType.Backup)]
        [InlineData(1, 3, BackupType.Snapshot)]
        [InlineData(64, 3, BackupType.Snapshot)]
        public async Task CanBackupAndRestoreDeletedRemoteAttachments(int attachmentsCount, int size, BackupType type)
        {
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();
                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_source"
                }))
                {
                    var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);

                    foreach (var attachment in Attachments)
                    {
                        await store.Operations.SendAsync(new DeleteAttachmentOperation(attachment.DocumentId, attachment.Name));
                    }

                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                    await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount);

                    // Perform backup
                    var backupPath = NewDataPath(suffix: "BackupFolder");
                    var config = Backup.CreateBackupConfiguration(backupPath, type);
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                    // Restore the backup
                    var restoredDatabaseName = GetDatabaseName();
                    using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = restoredDatabaseName
                    }))
                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    }.Initialize())
                    {
                        await PutRemoteAttachmentsConfiguration(restoredStore, Settings);

                        var restoredDatabase = await Databases.GetDocumentDatabaseInstanceFor(restoredStore);
                        using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var c = restoredDatabase.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).Count();
                            Assert.Equal(0, c);
                        }
                    }
                }
            }
        }

        [AzureRetryTheory]
        [InlineData(1, 3, BackupType.Backup)]
        [InlineData(64, 3, BackupType.Backup)]
        [InlineData(1, 3, BackupType.Snapshot)]
        [InlineData(64, 3, BackupType.Snapshot)]
        public async Task CanBackupAndRestoreRemoteAttachments(int attachmentsCount, int size, BackupType type)
        {
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();
                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_source"
                }))
                {
                    var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);
                    // Perform backup
                    var backupPath = NewDataPath(suffix: "BackupFolder");
                    var config = Backup.CreateBackupConfiguration(backupPath, type);
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                    // Restore the backup
                    var restoredDatabaseName = GetDatabaseName();
                    using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = restoredDatabaseName
                    }))
                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    }.Initialize())
                    {
                        await PutRemoteAttachmentsConfiguration(restoredStore, Settings);

                        var restoredDatabase = await Databases.GetDocumentDatabaseInstanceFor(restoredStore);
                        using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = restoredDatabase.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount, attachments.Count);

                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;
                                await GetAndCompareRemoteAttachment(restoredStore, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size, identifier);
                            });
                        }
                    }
                }
            }
        }

        [AzureRetryTheory]
        [InlineData(1, 1024 * 1024 * 10, BackupType.Backup)]
        [InlineData(3, 1024 * 1024 * 10, BackupType.Backup)]
        [InlineData(1, 1024 * 1024 * 10, BackupType.Snapshot)]
        [InlineData(3, 1024 * 1024 * 10, BackupType.Snapshot)]
        public async Task CanBackupAndRestoreLargeRemoteAttachments(int attachmentsCount, int size, BackupType type)
        {
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();
                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_source"
                }))
                {
                    var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);

                    // Perform backup
                    var backupPath = NewDataPath(suffix: "BackupFolder");
                    var config = Backup.CreateBackupConfiguration(backupPath, type);
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                    // Restore the backup
                    var restoredDatabaseName = GetDatabaseName();
                    using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = restoredDatabaseName
                    }))
                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    }.Initialize())
                    {
                        await PutRemoteAttachmentsConfiguration(restoredStore, Settings);

                        var restoredDatabase = await Databases.GetDocumentDatabaseInstanceFor(restoredStore);
                        using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = restoredDatabase.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount, attachments.Count);

                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;
                                await GetAndCompareRemoteAttachment(restoredStore, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size, identifier);
                            });
                        }
                    }
                }
            }
        }

        [AzureRetryTheory]
        [InlineData(64, 3, BackupType.Backup)]
        [InlineData(64, 3, BackupType.Snapshot)]
        public async Task CanBackupAndRestoreRemoteAttachmentsFromMultipleCollections(int attachmentsCount, int size, BackupType type)
        {
            Assert.True(attachmentsCount > 32, "this test meant to have more than 32 attachments so we will have more than one document");
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var collections = new List<string> { "Orders", "Products" };
                var ids = new List<(string Id, string Collection)>();
                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_source"
                }))
                {
                    var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, collections);

                    // Perform backup
                    var backupPath = NewDataPath(suffix: "BackupFolder");
                    var config = Backup.CreateBackupConfiguration(backupPath, type);
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                    // Restore the backup
                    var restoredDatabaseName = GetDatabaseName();
                    using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = restoredDatabaseName
                    }))
                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    }.Initialize())
                    {
                        await PutRemoteAttachmentsConfiguration(restoredStore, Settings, collections);

                        var restoredDatabase = await Databases.GetDocumentDatabaseInstanceFor(restoredStore);
                        using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = restoredDatabase.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount, attachments.Count);

                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;
                                await GetAndCompareRemoteAttachment(restoredStore, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size, identifier);
                            });
                        }
                    }
                }
            }
        }

        [AzureRetryTheory]
        [InlineData(1, 3, BackupType.Backup)]
        [InlineData(64, 3, BackupType.Backup)]
        [InlineData(1, 3, BackupType.Snapshot)]
        [InlineData(64, 3, BackupType.Snapshot)]
        public async Task CanBackupAndRestoreOverwrittenRemoteAttachmentWithIncrementalBackups(int attachmentsCount, int size, BackupType type)
        {
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();
                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_source"
                }))
                {
                    var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    using (var documentInfoHelper = new DocumentInfoHelper(context))
                    {
                        var attachments = database.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                        Assert.Equal(attachmentsCount, attachments.Count);

                        foreach (var attachment in attachments)
                        {
                            Assert.NotNull(attachment);

                            AddAttachmentToAttachments(documentInfoHelper, attachment);
                        }
                    }

                    // Perform initial backup
                    var backupPath = NewDataPath(suffix: "BackupFolder");
                    var config = Backup.CreateBackupConfiguration(backupPath, type, incrementalBackupFrequency: "0 0 * * *");
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                    config.TaskId = backupTaskId;
                    var newAttachments = attachmentsCount;
                    // Make some changes (e.g., add more attachments, remote them)
                    await PopulateDocsWithRandomAttachments(store, identifier, size, ids, attachmentsPerDoc);
                    Assert.Equal(attachmentsCount + newAttachments, Attachments.Count);

                    // move in time & start remote
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount + newAttachments, 15_000);

                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    using (var _documentInfoHelper = new DocumentInfoHelper(context))
                    {
                        var attachments = database.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();

                        Assert.Equal(newAttachments, attachments.Count);

                        foreach (var attachment in attachments)
                        {
                            Assert.NotNull(attachment);

                            using (var docId = _documentInfoHelper.GetDocumentId(attachment.Key))
                            {
                                var newAttachment = Attachments.FirstOrDefault(x =>
                                    x.DocumentId.ToLowerInvariant() == docId && x.Name == attachment.Name &&
                                    (x.RemoteParameters.IsLocalStorageAttachment()) &&
                                    x.Hash == attachment.Base64Hash.ToString());

                                Assert.NotNull(newAttachment);
                                var oldAttachment = Attachments.FirstOrDefault(x => x.DocumentId.ToLowerInvariant() == docId && x.Name == attachment.Name
                                    && x.RemoteParameters != null && x.Hash != attachment.Base64Hash.ToString());
                                Assert.NotNull(oldAttachment);

                                Attachments.Remove(newAttachment);
                                newAttachment.Key = attachment.Key;
                                newAttachment.Hash = attachment.Base64Hash.ToString();
                                newAttachment.RemoteParameters = new RemoteAttachmentParameters(attachment.RemoteParameters.Identifier, attachment.RemoteParameters.At);
                                newAttachment.RemoteKey = $"{Settings.RemoteFolderName}/{newAttachment.Hash}";
                                Attachments.Add(newAttachment);
                                Attachments.Remove(oldAttachment);

                                newAttachment.Stream.Position = 0;
                                await GetAndCompareRemoteAttachment(store, newAttachment.DocumentId, newAttachment.Name, newAttachment.Hash, newAttachment.ContentType, newAttachment.Stream, size, identifier);
                            }
                        }
                    }
                    Assert.Equal(attachmentsCount, Attachments.Count);

                    var stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(attachmentsCount, stats.CountOfRemoteAttachments);
                    Backup.RunBackup(Server, config.TaskId, store, isFullBackup: false);

                    // Restore the backup
                    var restoredDatabaseName = GetDatabaseName();
                    using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = restoredDatabaseName
                    }))
                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    }.Initialize())
                    {
                        await PutRemoteAttachmentsConfiguration(restoredStore, Settings);

                        var restoredDatabase = await Databases.GetDocumentDatabaseInstanceFor(restoredStore);
                        using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = restoredDatabase.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();

                            Assert.Equal(attachmentsCount, attachments.Count);

                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;
                                await GetAndCompareRemoteAttachment(restoredStore, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size, identifier);
                            });
                        }
                    }
                }
            }
        }

        [AzureRetryTheory]
        [InlineData(1, 3, BackupType.Backup)]
        [InlineData(64, 3, BackupType.Backup)]
        [InlineData(1, 3, BackupType.Snapshot)]
        [InlineData(64, 3, BackupType.Snapshot)]
        public async Task CanBackupAndRestoreRemoteAttachmentsWithIncrementalBackups(int attachmentsCount, int size, BackupType type)
        {
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();
                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_source"
                }))
                {
                   var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);

                    // Perform initial backup
                    var backupPath = NewDataPath(suffix: "BackupFolder");
                    var config = Backup.CreateBackupConfiguration(backupPath, type, incrementalBackupFrequency: "0 0 * * *");
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                    config.TaskId = backupTaskId;
                    // Make some changes (e.g., add more attachments, remote them)

                    await PopulateDocsWithRandomAttachments(store, identifier, size, ids, attachmentsPerDoc, start: attachmentsCount);

                    Assert.Equal(attachmentsCount * 2, Attachments.Count);

                    // move in time & start remote
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount * 2, 15_000);

                    GetStorageAttachmentsMetadataFromAllAttachments(database);

                    var stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(attachmentsCount * 2, stats.CountOfRemoteAttachments);

                    Backup.RunBackup(Server, config.TaskId, store, isFullBackup: false);
                    // Restore the backup
                    var restoredDatabaseName = GetDatabaseName();
                    using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = restoredDatabaseName
                    }))
                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    }.Initialize())
                    {
                        await PutRemoteAttachmentsConfiguration(restoredStore, Settings);

                        var restoredDatabase = await Databases.GetDocumentDatabaseInstanceFor(restoredStore);
                        using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = restoredDatabase.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount * 2, attachments.Count);

                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;
                                await GetAndCompareRemoteAttachment(restoredStore, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size, identifier);
                            });
                        }
                    }
                }
            }
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExportAndImportRemoteAttachmentsAsync(int attachmentsCount, int size)
        {
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();
                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_source"
                }))
                {
                    var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);

                    // Export
                    var exportPath = NewDataPath(suffix: "ExportFolder");
                    Directory.CreateDirectory(exportPath);
                    var exportFile = Path.Combine(exportPath, "export.ravendump");
                    await using (var exportStream = File.OpenWrite(exportFile))
                    {
                        var operation = await store.Smuggler.ExportAsync(new Raven.Client.Documents.Smuggler.DatabaseSmugglerExportOptions(), exportStream);
                        await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                    }

                    // Import
                    using (var importedStore = GetDocumentStore(new Options
                    {
                        ModifyDatabaseName = s => $"{s}_dest"
                    }))
                    {
                        var operation = await importedStore.Smuggler.ImportAsync(new Raven.Client.Documents.Smuggler.DatabaseSmugglerImportOptions(), exportFile);
                        await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                        var importedDatabase = await Databases.GetDocumentDatabaseInstanceFor(importedStore);
                        using (importedDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = importedDatabase.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount, attachments.Count);

                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;
                                await GetAndCompareRemoteAttachment(importedStore, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size, identifier);
                            });
                        }
                    }
                }
            }
        }

    }
}
