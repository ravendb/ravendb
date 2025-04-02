using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Schemas;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments
{
    public class S3RetiredAttachmentsBackupRestoreTests : RetiredAttachmentsS3Base
    {
        public S3RetiredAttachmentsBackupRestoreTests(ITestOutputHelper output) : base(output)
        {
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3, true)]
        [InlineData(64, 3, true)]
        [InlineData(1, 3, false)]
        [InlineData(64, 3, false)]
        public async Task CanBackupAndRestoreDeletedRetiredAttachments(int attachmentsCount, int size, bool storageOnly)
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
                    await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);

                    await PutRetireAttachmentsConfiguration(store, Settings);

                    foreach (var attachment in Attachments)
                    {
                        await store.Operations.SendAsync(new DeleteRetiredAttachmentOperation(attachment.DocumentId, attachment.Name, storageOnly: storageOnly));
                    }

                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                    if (storageOnly == false)
                    {
                        await GetBlobsFromCloudAndAssertForCount(Settings, 0);
                    }
                    else
                    {
                        await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount);
                    }

                    // Perform backup
                    var backupPath = NewDataPath(suffix: "BackupFolder");
                    var config = Backup.CreateBackupConfiguration(backupPath);
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                    // Restore the backup
                    var restoredDatabaseName = GetDatabaseName();
                     Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = restoredDatabaseName
                    });

                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    }.Initialize())
                    {
                        await PutRetireAttachmentsConfiguration(restoredStore, Settings);

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

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanBackupAndRestoreRetiredAttachments(int attachmentsCount, int size)
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
                    await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);

                    await PutRetireAttachmentsConfiguration(store, Settings);

                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                    await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount);

                    // Perform backup
                    var backupPath = NewDataPath(suffix: "BackupFolder");
                    var config = Backup.CreateBackupConfiguration(backupPath);
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                    // Restore the backup
                    var restoredDatabaseName = GetDatabaseName();
                    Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = restoredDatabaseName
                    });

                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    }.Initialize())
                    {
                        await PutRetireAttachmentsConfiguration(restoredStore, Settings);

                        var restoredDatabase = await Databases.GetDocumentDatabaseInstanceFor(restoredStore);
                        using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = restoredDatabase.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount, attachments.Count);

                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                // we loaded retired attachment from storage it doesn't have stream, so we populate it from the one we saved in test, so we can compare
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;

                                // this sends GetRetiredAttachmentOperation and compares the result
                                await GetAndCompareRetiredAttachment(restoredStore, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size);
                            });

                        }
                      
                    }
                }
            }
        }


        [AmazonS3RetryTheory]
        [InlineData(1, 1024 * 1024 * 10)] // 10 MB
        [InlineData(5, 1024 * 1024 * 50)] // 50 MB
        public async Task CanBackupAndRestoreLargeRetiredAttachments(int attachmentsCount, int size)
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
                    await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);

                    await PutRetireAttachmentsConfiguration(store, Settings);

                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                    await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount);

                    // Perform backup
                    var backupPath = NewDataPath(suffix: "BackupFolder");
                    var config = Backup.CreateBackupConfiguration(backupPath);
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                    // Restore the backup
                    var restoredDatabaseName = GetDatabaseName();
                    Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = restoredDatabaseName
                    });

                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    }.Initialize())
                    {
                        await PutRetireAttachmentsConfiguration(restoredStore, Settings);

                        var restoredDatabase = await Databases.GetDocumentDatabaseInstanceFor(restoredStore);
                        using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = restoredDatabase.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount, attachments.Count);

                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                // we loaded retired attachment from storage it doesn't have stream, so we populate it from the one we saved in test, so we can compare
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;

                                // this sends GetRetiredAttachmentOperation and compares the result
                                await GetAndCompareRetiredAttachment(restoredStore, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size);
                            });

                        }
                    }
                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(64, 3)]
        public async Task CanBackupAndRestoreRetiredAttachmentsFromMultipleCollections(int attachmentsCount, int size)
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
                    await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, collections);

                    await PutRetireAttachmentsConfiguration(store, Settings, collections);

                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                    await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount);

                    // Perform backup
                    var backupPath = NewDataPath(suffix: "BackupFolder");
                    var config = Backup.CreateBackupConfiguration(backupPath);
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                    // Restore the backup
                    var restoredDatabaseName = GetDatabaseName();
                    Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = restoredDatabaseName
                    });

                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    }.Initialize())
                    {
                        await PutRetireAttachmentsConfiguration(restoredStore, Settings, collections);

                        var restoredDatabase = await Databases.GetDocumentDatabaseInstanceFor(restoredStore);
                        using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = restoredDatabase.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount, attachments.Count);

                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                // we loaded retired attachment from storage it doesn't have stream, so we populate it from the one we saved in test, so we can compare
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;

                                // this sends GetRetiredAttachmentOperation and compares the result
                                await GetAndCompareRetiredAttachment(restoredStore, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size);
                            });

                        }
                    }
                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanBackupAndRestoreRetiredAttachmentsWithIncrementalBackups(int attachmentsCount, int size)
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
                    await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);

                    //TODO: egor this is already called in CanUploadRetiredAttachmentToCloudAndGetInternal
                    await PutRetireAttachmentsConfiguration(store, Settings);

                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                    await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount);

                    // Perform initial backup
                    var backupPath = NewDataPath(suffix: "BackupFolder");
                    var config = Backup.CreateBackupConfiguration(backupPath);
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                    // Make some changes (e.g., add more attachments, retire them)

                    await PopulateDocsWithRandomAttachments(store, size, ids, attachmentsPerDoc);
                    //TODO: egor here is a bug, we retired an attachment, then put attachment with same name, it update the attachmetn to be not retired, but we still have it in cloud and it have RetiredAt value !!!!
                    WaitForUserToContinueTheTest(store);
                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount * 2, Attachments.Count);

                    // move in time & start retire
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount * 2, 15_000);

                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    using (var _documentInfoHelper = new DocumentInfoHelper(context))

                    {

                        var attachments = database.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();

                        Assert.Equal(1, attachments.Count);

                        var attachment = attachments.FirstOrDefault();

                        Assert.NotNull(attachment);


                        using (var docId = _documentInfoHelper.GetDocumentId(attachment.Key))
                        {
                            //I have same attachmetn name & doc_id but flag is NONe
                            var t = Attachments.FirstOrDefault(x => x.DocumentId.ToLowerInvariant() == docId && x.Name == attachment.Name && x.Flags == AttachmentFlags.None);
                            Assert.NotNull(t);
                            Attachments.Remove(t);
                            t.Key = attachment.Key;
                            t.Hash = attachment.Base64Hash.ToString();
                            t.RetireAt = attachment.RetiredAt;
                            //        t.Flags = attachment.Flags;
                            //TODO: egor I can use getcollecton method here
                            t.RetiredKey =
                                $"{Settings.RemoteFolderName}/{t.Collection}/{Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(attachment.Key))}";
                            //  $"{Settings.RemoteFolderName}/{database.Name}/{t.Collection}/{Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(attachment.Key))}";
                            Attachments.Add(t);
                        }
                    }

                    await AssertAllRetiredAttachments(store, cloudObjects, size);

                    var stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(attachmentsCount, stats.CountOfRetiredAttachments);

                    //await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    //TODO: egor this is not creating incremental!!
                    // Perform incremental backup
                    config.IncrementalBackupFrequency = "0 0 * * *"; // Daily incremental backup
                    backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                    // Restore the backup
                    var restoredDatabaseName = GetDatabaseName();
                    Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = restoredDatabaseName
                    });

                    using (var restoredStore = new DocumentStore
                    {
                        Urls = store.Urls,
                        Database = restoredDatabaseName
                    }.Initialize())
                    {
                        await PutRetireAttachmentsConfiguration(restoredStore, Settings);

                        var restoredDatabase = await Databases.GetDocumentDatabaseInstanceFor(restoredStore);
                        using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = restoredDatabase.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount * 2, attachments.Count);

                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                // we loaded retired attachment from storage it doesn't have stream, so we populate it from the one we saved in test, so we can compare
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;

                                // this sends GetRetiredAttachmentOperation and compares the result
                                await GetAndCompareRetiredAttachment(restoredStore, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size);
                            });

                        }
                    }
                }
            }
        }
    }
}
