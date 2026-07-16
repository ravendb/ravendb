using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Voron;
using Voron.Data;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Issues;

public class RavenDB_26945(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Voron)]
    public async Task CompactionWillHaveGlobalIndexUpdated()
    {
        var backupPath = ExtractSnapshotToBackupFolder();

        using (var store = GetDocumentStore())
        {
            var databaseName = GetDatabaseName();

            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                   {
                       BackupLocation = backupPath,
                       DatabaseName = databaseName
                   }))
            {
                AssertStoredSchemaDeclaresFlagAndHash(await GetDatabase(databaseName));
                AssertAttachmentsFlagAndHash(await GetDatabase(databaseName));
                await CompactAsync(store, databaseName);
                AssertAttachmentsFlagAndHash(await GetDatabase(databaseName));
            }
        }
    }
    
    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Voron)]
    public async Task DeletingAttachmentsAfterCompactionOfAMigratedDatabaseMustWork()
    {
        var backupPath = ExtractSnapshotToBackupFolder();
        using var store = GetDocumentStore();
        var databaseName = GetDatabaseName();
        using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
               {
                   BackupLocation = backupPath,
                   DatabaseName = databaseName
               }))
        {
            await CompactAsync(store, databaseName);

            for (var group = 0; group < 3; group++)
            {
                for (var i = 0; i < 200; i++)
                {
                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        session.Delete($"orders/{group}-{i}");
                        await session.SaveChangesAsync();
                    }
                }
            }
        }
    }

    private string ExtractSnapshotToBackupFolder()
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var snapshotPath = Path.Combine(backupPath, "RavenDB_26945.ravendb-snapshot");

        using (var file = File.Create(snapshotPath))
        using (var stream = typeof(RavenDB_26945).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_26945.RavenDB_26945.ravendb-snapshot"))
        {
            Assert.NotNull(stream);
            stream.CopyTo(file);
        }

        return backupPath;
    }

    private static unsafe void AssertStoredSchemaDeclaresFlagAndHash(DocumentDatabase database)
    {
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var tx = context.Transaction.InnerTransaction;
            var tableTree = tx.ReadTree("AttachmentsMetadata", RootObjectType.Table);
            Assert.NotNull(tableTree);

            var schemaSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
            var schemaPtr = tableTree.DirectRead(TableSchema.SchemasSlice);
            var storedSchema = TableSchema.ReadFrom(tx.Allocator, schemaPtr, schemaSize);

            Assert.Contains(storedSchema.DynamicKeyIndexes, kvp => kvp.Key.ToString() == "AttachmentsFlagAndHash");
        }
    }

    private static async Task CompactAsync(Raven.Client.Documents.DocumentStore store, string databaseName)
    {
        var op = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
        {
            DatabaseName = databaseName,
            Documents = true
        }));
        await op.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
    }

    private static unsafe void AssertAttachmentsFlagAndHash(DocumentDatabase database)
    {
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var tx = context.Transaction.InnerTransaction;
            var indexTree = tx.ReadTree("AttachmentsFlagAndHash");
            Assert.NotNull(indexTree);

            var filePages = tx.LowLevelTransaction.DataPager.NumberOfAllocatedPages;

            using (var it = indexTree.Iterate(prefetch: false))
            {
                Assert.True(it.Seek(Slices.BeforeAllKeys));

                do
                {
                    var reader = it.CreateReaderForCurrent();
                    var ptr = reader.Base;
                    if (ptr == null || (RootObjectType)(*ptr) != RootObjectType.FixedSizeTree ||
                        reader.Length != sizeof(FixedSizeTreeHeader.Large))
                        continue; // embedded trees hold their data inline - verbatim copy is fine for them

                    var root = ((FixedSizeTreeHeader.Large*)ptr)->RootPageNumber;
                    Assert.True(root > 0);
                    Assert.True(root < filePages);
                    var page = tx.LowLevelTransaction.GetPage(root);
                    Assert.True((page.Flags & PageFlags.FixedSizeTreePage) == PageFlags.FixedSizeTreePage);
                } while (it.MoveNext());
            }
        }
    }
}
