using System;
using System.Collections;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Storage.Schema.Updates.Documents;
using SlowTests.Client.Attachments;
using Sparrow.Json;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;

namespace SlowTests.Server.Documents.Attachments
{
    public class RemoteAttachmentSchemaUpdateTests : RemoteAttachmentsS3Base
    {
        public RemoteAttachmentSchemaUpdateTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task Can_Open_v71_Data()
        {
            var dest = "Northwind";
            var snapshot = $"{dest}.zip";
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, snapshot);
            var databasePath = Path.Combine(backupPath, dest);

            await using (var file = File.Create(fullBackupPath))
            {
                await using (var stream = typeof(RemoteAttachmentSchemaUpdateTests).Assembly.GetManifestResourceStream($"SlowTests.Data.Attachments.RavenDB_24543.{snapshot}"))
                {
                    Assert.NotNull(stream);
                    await stream.CopyToAsync(file);
                }
            }

            var zipPath = new PathSetting(fullBackupPath);
            Assert.True(File.Exists(zipPath.FullPath));

            ZipFile.ExtractToDirectory(zipPath.FullPath, backupPath);
            using (var store2 = GetDocumentStore())
            using (var store = GetDocumentStore(new Options { CreateDatabase = false, RunInMemory = false, }))
            {
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(store.Database) { Settings = { ["DataDir"] = databasePath, ["RunInMemory"] = "false" } }));

                var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                Assert.Equal(0, stats.CountOfRemoteAttachments);
                Assert.Equal(17, stats.CountOfAttachments);
                Assert.Equal(16, stats.CountOfUniqueAttachments);
                Assert.Equal(17, stats.CountOfDocuments);
                Assert.Equal(0, stats.CountOfTimeSeriesSegments);
                using (var stream = GetType().Assembly.GetManifestResourceStream("SlowTests.Data.Attachments.RavenDB_24543.DumpofNorthwind.ravendbdump"))
                {
                    var operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(new DatabaseSmugglerImportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Attachments
                    }), stream);
                    operation.WaitForCompletion<SmugglerResult>(TimeSpan.FromSeconds(30));
                }

                var stats2 = await store2.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                Assert.Equal(0, stats2.CountOfRemoteAttachments);
                Assert.Equal(17, stats2.CountOfAttachments);
                Assert.Equal(16, stats2.CountOfUniqueAttachments);

                var idsStr =
                    "\"categories/1-A\", \"categories/2-A\", \"categories/3-A\", \"categories/4-A\", \"categories/5-A\", \"categories/6-A\", \"categories/7-A\", \"categories/8-A\", \"employees/9-A\", \"employees/1-A\", \"employees/2-A\", \"employees/3-A\", \"employees/4-A\", \"employees/5-A\", \"employees/6-A\", \"employees/7-A\", \"employees/8-A\"";

                var ids = idsStr.Split(',')
                    .Select(id => id.Trim().Trim('"'))
                    .ToList();

                using (var session = store.OpenAsyncSession())
                {
                    // Query all documents with attachments
                    var documentsWithAttachments = await session.Advanced.AsyncRawQuery<dynamic>(@"
                                                                                                    from @all_docs 
                                                                                                    where true and exists(@metadata.@attachments)
                                                                                                  ").ToListAsync();

                    Assert.True(documentsWithAttachments.Count > 0, "Should have documents with attachments");

                    var str = new StringBuilder();
                    foreach (var doc in documentsWithAttachments)
                    {
                        var metadata = session.Advanced.GetMetadataFor(doc);
                        var documentId = metadata.GetString(Constants.Documents.Metadata.Id);
                        str.Append($"\"{documentId}\", ");
                    }

                    str.Length -= 2; // Remove trailing comma and space
                    var ids2 = str.ToString().Split(',')
                        .Select(id => id.Trim().Trim('"'))
                        .ToList();

                    Assert.Equal(ids.Count, ids2.Count);
                    Assert.All(ids2, id => Assert.Contains(id, ids));
                }

                Assert.Equal(17, ids.Count);

                using (var commands = store.Commands())
                {
                    foreach (var id in ids)
                    {
                        dynamic doc = await commands.GetAsync(id, true);
                        Assert.NotNull(doc);

                        var metadataObj = doc["@metadata"];
                        Assert.NotNull(metadataObj);
                        var attachmentsObj = metadataObj["@attachments"];
                        Assert.NotNull(attachmentsObj);
                        // If it's a DynamicArray, we need to iterate differently
                        var attachmentsArray = attachmentsObj as IEnumerable;
                        Assert.NotNull(attachmentsArray);
                        foreach (var attachmentInfo in attachmentsArray)
                        {
                            dynamic attachment = attachmentInfo;
                            var attachmentName = attachment.Name.ToString();
                            var attachmentHash = attachment.Hash.ToString();
                            var attachmentContentType = attachment.ContentType.ToString();
                            var attachmentSize = (long)attachment.Size;

                            using (var session = store.OpenAsyncSession())
                            using (var session2 = store2.OpenAsyncSession())
                            {
                                // Get the actual attachment after schema update
                                using AttachmentResult attachmentResult = await session.Advanced.Attachments.GetAsync(id, attachmentName);
                                Assert.NotNull(attachmentResult);
                                Assert.Equal(attachmentName, attachmentResult.Details.Name);
                                Assert.Equal(attachmentHash, attachmentResult.Details.Hash);
                                Assert.Equal(attachmentContentType, attachmentResult.Details.ContentType);
                                Assert.Equal(attachmentSize, attachmentResult.Details.Size);

                                Assert.Null(attachmentResult.Details.RemoteParameters);
                                Assert.False(attachment.RemoteParameters.IsExplicitNull); // RemoteParameters should not exist for existing attachments

                                // Get the actual attachment from sample data
                                using AttachmentResult attachmentResult2 = await session2.Advanced.Attachments.GetAsync(id, attachmentName);
                                Assert.NotNull(attachmentResult2);
                                Assert.Equal(attachmentResult.Details.Name, attachmentResult2.Details.Name);
                                Assert.Equal(attachmentResult.Details.Hash, attachmentResult2.Details.Hash);
                                Assert.Equal(attachmentResult.Details.ContentType, attachmentResult2.Details.ContentType);
                                Assert.Equal(attachmentResult.Details.Size, attachmentResult2.Details.Size);
                                Assert.Equal(attachmentResult.Details.RemoteParameters, attachmentResult2.Details.RemoteParameters);

                                // Read attachment content
                                await using (var stream = new MemoryStream())
                                await using (var stream2 = new MemoryStream())
                                {
                                    await attachmentResult.Stream.CopyToAsync(stream);
                                    stream.Position = 0;
                                    await attachmentResult2.Stream.CopyToAsync(stream2);
                                    stream2.Position = 0;
                                    AttachmentsStreamTests.CompareStreams(stream, stream2);
                                }
                            }
                        }
                    }
                }

                // CRUD with schema updated attachment
                using (var session = store.OpenAsyncSession())
                {
                    var idsStr1 = ids.FirstOrDefault();

                    // Put new attachment
                    var newContent = "This is a test attachment content";
                    var newContentBytes = Encoding.UTF8.GetBytes(newContent);
                    using (var newStream = new MemoryStream(newContentBytes))
                    {
                        store.Operations.Send(new PutAttachmentOperation(idsStr1, "test-attachment.txt", newStream, "text/plain"));
                    }

                    // Verify new attachment was added
                    var newAttachment = await session.Advanced.Attachments.GetAsync(idsStr1, "test-attachment.txt");
                    Assert.NotNull(newAttachment);
                    Assert.Equal("test-attachment.txt", newAttachment.Details.Name);
                    Assert.Equal("text/plain", newAttachment.Details.ContentType);

                    using (var stream = newAttachment.Stream)
                    {
                        var retrievedContent = await new StreamReader(stream).ReadToEndAsync();
                        Assert.Equal(newContent, retrievedContent);
                    }

                    // Delete attachment
                    await store.Operations.SendAsync(new DeleteAttachmentOperation(idsStr1, "test-attachment.txt"));

                    // Verify attachment was deleted
                    var deletedAttachment = await session.Advanced.Attachments.GetAsync(idsStr1, "test-attachment.txt");
                    Assert.Null(deletedAttachment);
                }

                // Verify final stats after operations
                var finalStats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                Assert.Equal(0, finalStats.CountOfRemoteAttachments);
                Assert.Equal(17, finalStats.CountOfAttachments); // Should remain same after delete
                Assert.Equal(16, finalStats.CountOfUniqueAttachments);
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task Schema_Update_From62000_Should_Add_RemoteAttachment_Fields()
        {
            // Test that the schema update properly adds the new fields for remote attachments
            var dest = "Northwind";
            var snapshot = $"{dest}.zip";
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, snapshot);
            var databasePath = Path.Combine(backupPath, dest);

            await using (var file = File.Create(fullBackupPath))
            {
                await using (var stream = typeof(RemoteAttachmentSchemaUpdateTests).Assembly.GetManifestResourceStream($"SlowTests.Data.Attachments.RavenDB_24543.{snapshot}"))
                {
                    Assert.NotNull(stream);
                    await stream.CopyToAsync(file);
                }
            }

            ZipFile.ExtractToDirectory(fullBackupPath, backupPath);

            using (var store = GetDocumentStore(new Options { CreateDatabase = false, RunInMemory = false, }))
            {
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(store.Database)
                {
                    Settings = { ["DataDir"] = databasePath, ["RunInMemory"] = "false" }
                }));

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                // Test 1: Verify AttachmentsFlagAndHash dynamic index was created
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var attachmentTable = context.Transaction.InnerTransaction.OpenTable(Raven.Server.Documents.Schemas.Attachments.AttachmentsSchemaBase, Raven.Server.Documents.Schemas.Attachments.AttachmentsMetadataSlice);
                    Assert.NotNull(attachmentTable);

                    // Verify the dynamic index exists
                    Assert.True(Raven.Server.Documents.Schemas.Attachments.AttachmentsSchemaBase.DynamicKeyIndexes.ContainsKey(Raven.Server.Documents.Schemas.Attachments.AttachmentsFlagAndHashSlice));

                    var dynamicIndex = Raven.Server.Documents.Schemas.Attachments.AttachmentsSchemaBase.DynamicKeyIndexes[Raven.Server.Documents.Schemas.Attachments.AttachmentsFlagAndHashSlice];
                    Assert.NotNull(dynamicIndex);
                    Assert.Equal(nameof(RemoteAttachmentsStorage.GenerateFlagAndHashForAttachments), dynamicIndex.GenerateKey.Method.Name);
                    var tree = attachmentTable.GetTree(dynamicIndex);
                    Assert.Equal(16, tree.State.Header.NumberOfEntries);

                    // Test 2: Verify all existing attachments have RemoteAttachmentFlags.None
                    foreach (var result in attachmentTable.SeekForwardFrom(Raven.Server.Documents.Schemas.Attachments.AttachmentsSchemaBase.FixedSizeIndexes[Raven.Server.Documents.Schemas.Attachments.AttachmentsEtagSlice], 0, 0))
                    {
                        var attachment = TableValueToAttachmentTableValueReader(context, ref result.Reader);
                        // Verify all migrated attachments have RemoteAttachmentFlags.None
                        Assert.Equal(RemoteAttachmentFlags.None, attachment.Flags);

                        Assert.True(attachment.Size > 0, "attachment.Size > 0");
                        Assert.True(string.IsNullOrEmpty(attachment.Identifier), "string.IsNullOrEmpty(attachment.Identifier)");
                        Assert.Null(attachment.RemoteAt);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task Schema_Update_From62000_Should_Handle_Batch_Processing()
        {
            // Test the batch processing mechanism in the schema update

            // Set a smaller batch size for testing
            var originalBatchSize = From62000.NumberOfAttachmentsToMigrateInSingleTransaction;
            From62000.NumberOfAttachmentsToMigrateInSingleTransaction = 1; // Small batch for testing

            try
            {
                var dest = "Northwind";
                var snapshot = $"{dest}.zip";
                var backupPath = NewDataPath(forceCreateDir: true);
                var fullBackupPath = Path.Combine(backupPath, snapshot);
                var databasePath = Path.Combine(backupPath, dest);

                await using (var file = File.Create(fullBackupPath))
                {
                    await using (var stream = typeof(RemoteAttachmentSchemaUpdateTests).Assembly.GetManifestResourceStream($"SlowTests.Data.Attachments.RavenDB_24543.{snapshot}"))
                    {
                        Assert.NotNull(stream);
                        await stream.CopyToAsync(file);
                    }
                }

                ZipFile.ExtractToDirectory(fullBackupPath, backupPath);

                using (var store = GetDocumentStore(new Options { CreateDatabase = false, RunInMemory = false, }))
                {
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(store.Database)
                    {
                        Settings = { ["DataDir"] = databasePath, ["RunInMemory"] = "false" }
                    }));

                    var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                    Assert.Equal(17, stats.CountOfAttachments); // Should have all attachments after migration
                    Assert.Equal(16, stats.CountOfUniqueAttachments);

                    // Verify all attachments are still functional
                    using (var session = store.OpenAsyncSession())
                    {
                        var documentsWithAttachments = await session.Advanced.AsyncRawQuery<dynamic>(@"
                    from @all_docs 
                    where exists(@metadata.@attachments)
                ").ToListAsync();

                        Assert.True(documentsWithAttachments.Count > 0);

                        foreach (var doc in documentsWithAttachments)
                        {
                            var metadata = session.Advanced.GetMetadataFor(doc);
                            string documentId = metadata[Constants.Documents.Metadata.Id];
                            var attachments = metadata[Constants.Documents.Metadata.Attachments];
                            Assert.NotNull(attachments);
                            foreach (var attachmentInfo in attachments)
                            {
                                string attachmentName = attachmentInfo["Name"];
                                var attachment = await session.Advanced.Attachments.GetAsync(documentId, attachmentName);
                                Assert.NotNull(attachment);
                            }
                        }
                    }
                }
            }
            finally
            {
                // Restore original batch size
                From62000.NumberOfAttachmentsToMigrateInSingleTransaction = originalBatchSize;
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task Schema_Update_From62000_Should_Create_Proper_Dynamic_Index_Tree()
        {
            // Test that the AttachmentsFlagAndHash index tree is properly created

            var dest = "Northwind";
            var snapshot = $"{dest}.zip";
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, snapshot);
            var databasePath = Path.Combine(backupPath, dest);

            await using (var file = File.Create(fullBackupPath))
            {
                await using (var stream = typeof(RemoteAttachmentSchemaUpdateTests).Assembly.GetManifestResourceStream($"SlowTests.Data.Attachments.RavenDB_24543.{snapshot}"))
                {
                    Assert.NotNull(stream);
                    await stream.CopyToAsync(file);
                }
            }

            ZipFile.ExtractToDirectory(fullBackupPath, backupPath);

            using (var store = GetDocumentStore(new Options { CreateDatabase = false, RunInMemory = false, }))
            {
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(store.Database)
                {
                    Settings = { ["DataDir"] = databasePath, ["RunInMemory"] = "false" }
                }));

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                AssertNumberOfEntriesInDynamicIndex(database, 16);

                // delete document with id "employees/8-a"
                using (var session = store.OpenAsyncSession())
                {
                    session.Delete("employees/8-A");
                    await session.SaveChangesAsync();
                }

                AssertNumberOfEntriesInDynamicIndex(database, 16); // we should still have 16 entries, because the index supports duplicate keys

                // delete document with id "employees/7-a"
                using (var session = store.OpenAsyncSession())
                {
                    session.Delete("employees/7-A");
                    await session.SaveChangesAsync();
                }
                AssertNumberOfEntriesInDynamicIndex(database, 15);
            }
        }

        private static void AssertNumberOfEntriesInDynamicIndex(DocumentDatabase database, int expected)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                // Verify the index tree was created
                var indexTree = context.Transaction.InnerTransaction.ReadTree(Raven.Server.Documents.Schemas.Attachments.AttachmentsFlagAndHashSlice);
                Assert.NotNull(indexTree);

                // Verify it contains entries for all attachments
                using (var iterator = indexTree.Iterate(false))
                {
                    var indexEntryCount = 0;
                    if (iterator.Seek(Slices.BeforeAllKeys))
                    {
                        do
                        {
                            indexEntryCount++;
                        } while (iterator.MoveNext());
                    }

                    // Should have index entries for all attachments
                    Assert.True(indexEntryCount > 0);
                    Assert.Equal(expected, indexEntryCount); // Based on the known test data
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task Schema_Update_From62000_Should_Maintain_Document_Attachment_Metadata()
        {
            // Test that document metadata @attachments arrays are still valid after migration

            var dest = "Northwind";
            var snapshot = $"{dest}.zip";
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, snapshot);
            var databasePath = Path.Combine(backupPath, dest);

            await using (var file = File.Create(fullBackupPath))
            {
                await using (var stream = typeof(RemoteAttachmentSchemaUpdateTests).Assembly.GetManifestResourceStream($"SlowTests.Data.Attachments.RavenDB_24543.{snapshot}"))
                {
                    Assert.NotNull(stream);
                    await stream.CopyToAsync(file);
                }
            }

            ZipFile.ExtractToDirectory(fullBackupPath, backupPath);

            using (var store = GetDocumentStore(new Options { CreateDatabase = false, RunInMemory = false, }))
            {
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(store.Database)
                {
                    Settings = { ["DataDir"] = databasePath, ["RunInMemory"] = "false" }
                }));

                using (var session = store.OpenAsyncSession())
                {
                    var documentsWithAttachments = await session.Advanced.AsyncRawQuery<dynamic>(@"
                from @all_docs 
                where exists(@metadata.@attachments)
            ").ToListAsync();

                    foreach (var doc in documentsWithAttachments)
                    {
                        var metadata = session.Advanced.GetMetadataFor(doc);
                        var documentId = metadata[Constants.Documents.Metadata.Id];

                        // Verify document has HasAttachments flag
                        var flags = metadata[Constants.Documents.Metadata.Flags];
                        Assert.Contains("HasAttachments", flags);
                        var attachments = metadata[Constants.Documents.Metadata.Attachments];
                        // Verify @attachments metadata matches actual attachments
                        Assert.NotNull(attachments);
                        foreach (var attachmentInfo in attachments)
                        {
                            string attachmentName = attachmentInfo["Name"];
                            string attachmentHash = attachmentInfo["Hash"];
                            var attachmentSize = attachmentInfo["Size"];
                            var attachmentContentType = attachmentInfo["ContentType"];

                            // Verify actual attachment exists and matches metadata
                            var attachment = await session.Advanced.Attachments.GetAsync(documentId, attachmentName);
                            Assert.NotNull(attachment);
                            Assert.Equal(attachmentName, attachment.Details.Name);
                            Assert.Equal(attachmentHash, attachment.Details.Hash);
                            Assert.Equal(attachmentSize, attachment.Details.Size);
                            Assert.Equal(attachmentContentType, attachment.Details.ContentType);
                        }
                    }
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task Schema_Update_From62000_Should_Allow_New_Remote_Attachment_Operations()
        {
            // Test that remote attachment operations work after the schema update

            var dest = "Northwind";
            var snapshot = $"{dest}.zip";
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, snapshot);
            var databasePath = Path.Combine(backupPath, dest);

            await using (var file = File.Create(fullBackupPath))
            {
                await using (var stream = typeof(RemoteAttachmentSchemaUpdateTests).Assembly.GetManifestResourceStream($"SlowTests.Data.Attachments.RavenDB_24543.{snapshot}"))
                {
                    Assert.NotNull(stream);
                    await stream.CopyToAsync(file);
                }
            }

            ZipFile.ExtractToDirectory(fullBackupPath, backupPath);

            using (var store = GetDocumentStore(new Options { CreateDatabase = false, RunInMemory = false, }))
            {
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(store.Database)
                {
                    Settings = { ["DataDir"] = databasePath, ["RunInMemory"] = "false" }
                }));

                // Test that remote attachment functionality works after migration
                var testDocId = "test-doc";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new { Name = "Test" }, testDocId);
                    await session.SaveChangesAsync();
                }
                // Add an attachment that can be remote
                var content = "This is test attachment content for remotement";
                var contentBytes = Encoding.UTF8.GetBytes(content);
                using (var stream = new MemoryStream(contentBytes))
                {
                    store.Operations.Send(new PutAttachmentOperation(testDocId, "test-attachment.txt", stream, "text/plain"));

                    // Test that the attachment was stored with correct schema (including new fields)
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var attachmentTable = context.Transaction.InnerTransaction.OpenTable(Raven.Server.Documents.Schemas.Attachments.AttachmentsSchemaBase, Raven.Server.Documents.Schemas.Attachments.AttachmentsMetadataSlice);
                        Assert.NotNull(attachmentTable);
                        foreach (var result in attachmentTable.SeekForwardFrom(Raven.Server.Documents.Schemas.Attachments.AttachmentsSchemaBase.FixedSizeIndexes[Raven.Server.Documents.Schemas.Attachments.AttachmentsEtagSlice], 0, 0))
                        {
                            var attachment = TableValueToAttachmentTableValueReader(context, ref result.Reader);
                            // Verify all migrated attachments have RemoteAttachmentFlags.None
                            Assert.Equal(RemoteAttachmentFlags.None, attachment.Flags);

                            Assert.True(attachment.Size > 0, "attachment.Size > 0");
                            Assert.True(string.IsNullOrEmpty(attachment.Identifier), "string.IsNullOrEmpty(attachment.Identifier)");
                            Assert.Null(attachment.RemoteAt);
                        }
                    }

                    // Verify attachment is accessible
                    using (var session = store.OpenAsyncSession())
                    {
                        var attachmentResult = await session.Advanced.Attachments.GetAsync(testDocId, "test-attachment.txt");
                        Assert.NotNull(attachmentResult);
                        Assert.Equal("test-attachment.txt", attachmentResult.Details.Name);

                        using (var stream2 = attachmentResult.Stream)
                        {
                            var retrievedContent = await new StreamReader(stream2).ReadToEndAsync();
                            Assert.Equal(content, retrievedContent);
                        }
                    }

                    //verify that we can remote the attachment
                    await using (var holder = CreateCloudSettings())
                    {
                        var remoteAt = DateTime.UtcNow.AddMinutes(3);
                        var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);

                        stream.Position = 0;
                        store.Operations.Send(new PutAttachmentOperation(testDocId, "test-attachment.txt", stream, "text/plain",
                            remoteParameters: new RemoteAttachmentParameters(identifier, remoteAt)));

                        database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                        database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                        await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                        using (var session = store.OpenAsyncSession())
                        {
                            var attachmentResult = await session.Advanced.Attachments.GetAsync(testDocId, "test-attachment.txt");
                            Assert.NotNull(attachmentResult);
                            Assert.Equal("test-attachment.txt", attachmentResult.Details.Name);

                            Assert.NotNull(attachmentResult.Details.RemoteParameters);
                            Assert.Equal(identifier, attachmentResult.Details.RemoteParameters.Identifier);
                            Assert.Equal(remoteAt, attachmentResult.Details.RemoteParameters.At);
                            Assert.Equal(RemoteAttachmentFlags.Remote, attachmentResult.Details.RemoteParameters.Flags);
                        }
                    }
                }
            }
        }

        internal sealed class AttachmentTableValueReader
        {
            public long StorageId;
            public LazyStringValue Key;
            public long Etag;
            public string ChangeVector;
            public LazyStringValue Name;
            public LazyStringValue ContentType;
            public Slice Base64Hash;
            public Stream Stream;
            public short TransactionMarker;
            public long Size;
            public string Identifier;
            public DateTime? RemoteAt;
            public RemoteAttachmentFlags Flags;
        }

        internal static unsafe AttachmentTableValueReader TableValueToAttachmentTableValueReader(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var result = new AttachmentTableValueReader
            {
                StorageId = tvr.Id,
                Key = DocumentsStorage.TableValueToString(context, (int)Raven.Server.Documents.Schemas.Attachments.AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, ref tvr),
                Etag = DocumentsStorage.TableValueToEtag((int)Raven.Server.Documents.Schemas.Attachments.AttachmentsTable.Etag, ref tvr),
                ChangeVector = DocumentsStorage.TableValueToChangeVector(context, (int)Raven.Server.Documents.Schemas.Attachments.AttachmentsTable.ChangeVector, ref tvr),
                Name = DocumentsStorage.TableValueToId(context, (int)Raven.Server.Documents.Schemas.Attachments.AttachmentsTable.Name, ref tvr),
                ContentType = DocumentsStorage.TableValueToId(context, (int)Raven.Server.Documents.Schemas.Attachments.AttachmentsTable.ContentType, ref tvr),
                Size = DocumentsStorage.TableValueToLong((int)Raven.Server.Documents.Schemas.Attachments.AttachmentsTable.Size, ref tvr),
                Identifier = DocumentsStorage.TableValueToString(context, (int)Raven.Server.Documents.Schemas.Attachments.AttachmentsTable.Identifier, ref tvr),
                RemoteAt = DocumentsStorage.TableValueToNullableDateTime((int)Raven.Server.Documents.Schemas.Attachments.AttachmentsTable.RemoteAt, ref tvr),
                Flags = DocumentsStorage.TableValueToAttachmentFlags((int)Raven.Server.Documents.Schemas.Attachments.AttachmentsTable.Flags, ref tvr)
            };

            DocumentsStorage.TableValueToSlice(context, (int)Raven.Server.Documents.Schemas.Attachments.AttachmentsTable.Hash, ref tvr, out result.Base64Hash);

            result.TransactionMarker = *(short*)tvr.Read((int)Raven.Server.Documents.Schemas.Attachments.AttachmentsTable.TransactionMarker, out int _);

            return result;
        }
    }
}
