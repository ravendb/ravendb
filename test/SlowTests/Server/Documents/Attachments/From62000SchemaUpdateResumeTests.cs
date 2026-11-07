using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Storage.Schema.Updates.Documents;
using Sparrow.Binary;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Xunit;
using AttachmentsSchemas = Raven.Server.Documents.Schemas.Attachments;

namespace SlowTests.Server.Documents.Attachments
{
    public class From62000SchemaUpdateResumeTests : RavenTestBase
    {
        public From62000SchemaUpdateResumeTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(1, 5)] // interrupted while the AttachmentsFlagAndHash index is being populated
        [InlineData(1, 20)] // interrupted while the records are being migrated
        [InlineData(3, 7)] // batches that do not divide the 17 attachments evenly
        public async Task Schema_Update_From62000_Interrupted_And_Rerun_Produces_The_Same_Result(int batchSize, int interruptAfterBatches)
        {
            var originalBatchSize = From62000.NumberOfAttachmentsToMigrateInSingleTransaction;
            From62000.NumberOfAttachmentsToMigrateInSingleTransaction = batchSize;
            try
            {
                List<string> expected = await UpgradeAndDump(interruptAfterBatches: null);
                List<string> actual = await UpgradeAndDump(interruptAfterBatches);

                Assert.Equal(expected, actual);
                Assert.Contains("NumberOfEntries=17", expected);
                Assert.Contains("FlagAndHashEntries=17", expected); // 17 attachments, two of them share a hash
            }
            finally
            {
                From62000.NumberOfAttachmentsToMigrateInSingleTransaction = originalBatchSize;
                From62000.ForTestingPurposes = null;
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task Schema_Update_From62000_Stores_The_Stream_Size_Of_Every_Attachment()
        {
            var databasePath = ExtractNorthwindSnapshot();

            using (var store = GetDocumentStore(new Options { CreateDatabase = false, RunInMemory = false }))
            {
                CreateDatabase(store, databasePath);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tx = context.Transaction.InnerTransaction;
                    var table = tx.OpenTable(database.DocumentsStorage.AttachmentsStorage.AttachmentsSchema, AttachmentsSchemas.AttachmentsMetadataSlice);
                    var streamsTree = tx.ReadTree(AttachmentsSchemas.AttachmentsSlice);

                    var count = 0;
                    foreach (var holder in table.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
                    {
                        count++;
                        var attachment = AttachmentsStorage.TableValueToAttachment(context, ref holder.Reader);
                        long sizeFromStreamInfo = ReadSizeFromStreamInfo(streamsTree, attachment.Base64Hash);

                        Assert.True(attachment.Size > 0, $"attachment {attachment.Key} has size {attachment.Size}");
                        Assert.Equal(sizeFromStreamInfo, attachment.Size);
                        Assert.Equal(attachment.Size, streamsTree.ReadStream(attachment.Base64Hash).Length);
                    }

                    Assert.Equal(17, count);
                }
            }
        }

        private static unsafe long ReadSizeFromStreamInfo(Tree streamsTree, Slice hash)
        {
            Tree.StreamInfo* info = streamsTree.GetStreamInfo(hash, writable: false);
            Assert.True(info != null);
            return info->TotalSize;
        }

        private async Task<List<string>> UpgradeAndDump(int? interruptAfterBatches)
        {
            var databasePath = ExtractNorthwindSnapshot();

            using (var store = GetDocumentStore(new Options { CreateDatabase = false, RunInMemory = false }))
            {
                if (interruptAfterBatches != null)
                {
                    From62000.ForTestingPurposesOnly().AfterBatchCommit = committedBatches =>
                    {
                        if (committedBatches == interruptAfterBatches)
                            throw new InvalidOperationException("Simulated crash during the schema upgrade");
                    };

                    var error = await Assert.ThrowsAnyAsync<Exception>(async () =>
                    {
                        CreateDatabase(store, databasePath);
                        await Databases.GetDocumentDatabaseInstanceFor(store);
                    });
                    Assert.Contains("Simulated crash during the schema upgrade", error.ToString());

                    From62000.ForTestingPurposes = null;

                    // The catastrophic failure handler unloads the faulted database shortly after the error. Wait for that, so the
                    // retry below is not unloaded underneath us. The database is then loaded again over the partially migrated
                    // files, and the upgrade starts from the beginning, exactly like a server restart after a crash.
                    var landlord = Server.ServerStore.DatabasesLandlord;
                    await Task.Delay(landlord.CatastrophicFailureHandler.TimeToWaitBeforeUnloadingDatabase + TimeSpan.FromSeconds(1));
                    var stillCached = await WaitForValueAsync(() => Task.FromResult(landlord.DatabasesCache.TryGetValue(store.Database, out _)), false, timeout: 30_000);
                    Assert.False(stillCached);

                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    if (record == null)
                        CreateDatabase(store, databasePath);
                }
                else
                {
                    CreateDatabase(store, databasePath);
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                return DumpAttachmentsTable(database);
            }
        }

        private static void CreateDatabase(Raven.Client.Documents.IDocumentStore store, string databasePath)
        {
            store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(store.Database)
            {
                Settings = { ["DataDir"] = databasePath, ["RunInMemory"] = "false" }
            }));
        }

        private string ExtractNorthwindSnapshot()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "Northwind.zip");

            using (var file = File.Create(fullBackupPath))
            using (var stream = typeof(RemoteAttachmentSchemaUpdateTests).Assembly.GetManifestResourceStream("SlowTests.Data.Attachments.RavenDB_24543.Northwind.zip"))
            {
                Assert.NotNull(stream);
                stream.CopyTo(file);
            }

            ZipFile.ExtractToDirectory(fullBackupPath, backupPath);
            return Path.Combine(backupPath, "Northwind");
        }

        // Every record with all of its fields, and every index entry resolved to the primary key of the record it points to.
        // Storage ids and page layout are not part of the dump, because they legitimately differ between runs.
        internal static unsafe List<string> DumpAttachmentsTable(DocumentDatabase database)
        {
            var lines = new List<string>();

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var tx = context.Transaction.InnerTransaction;
                var schema = database.DocumentsStorage.AttachmentsStorage.AttachmentsSchema;
                var table = tx.OpenTable(schema, AttachmentsSchemas.AttachmentsMetadataSlice);

                lines.Add($"NumberOfEntries={table.NumberOfEntries}");

                foreach (var holder in table.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
                {
                    var reader = holder.Reader;
                    var fields = new List<string> { $"record:{reader.Count}" };
                    for (int i = 0; i < reader.Count; i++)
                    {
                        var ptr = reader.Read(i, out int size);
                        fields.Add(Convert.ToHexString(new ReadOnlySpan<byte>(ptr, size)));
                    }

                    lines.Add(string.Join("|", fields));
                }

                foreach (var result in table.SeekForwardFrom(schema.FixedSizeIndexes[AttachmentsSchemas.AttachmentsEtagSlice], 0, 0))
                    lines.Add($"etag:{Bits.SwapBytes(*(long*)result.Reader.Read((int)AttachmentsSchemas.AttachmentsTable.Etag, out _))}|{PrimaryKey(result.Reader)}");

                foreach (var result in table.SeekForwardFrom(schema.Indexes[AttachmentsSchemas.AttachmentsHashSlice], Slices.BeforeAllKeys, 0))
                    lines.Add($"hash:{result.Key}|{PrimaryKey(result.Result.Reader)}");

                long flagAndHashEntries = 0;
                foreach (var result in table.SeekForwardFrom(schema.DynamicKeyIndexes[AttachmentsSchemas.AttachmentsFlagAndHashSlice], Slices.BeforeAllKeys, 0))
                {
                    flagAndHashEntries++;
                    lines.Add($"flagandhash:{Convert.ToHexString(result.Key.AsReadOnlySpan())}|{PrimaryKey(result.Result.Reader)}");
                }

                lines.Add($"FlagAndHashEntries={flagAndHashEntries}");
            }

            lines.Sort(StringComparer.Ordinal);
            return lines;
        }

        private static unsafe string PrimaryKey(TableValueReader reader)
        {
            var ptr = reader.Read((int)AttachmentsSchemas.AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, out int size);
            return Convert.ToHexString(new ReadOnlySpan<byte>(ptr, size));
        }
    }
}
