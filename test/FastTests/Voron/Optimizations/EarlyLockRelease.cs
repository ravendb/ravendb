using System.IO;
using System.Text;
using Raven.Server.ServerWide.Context;
using Voron.Data.Tables;
using Xunit;

namespace FastTests.Voron.Optimizations
{
    public class EarlyLockRelease : RavenLowLevelTestBase
    {
        [Fact]
        public void ShouldWork()
        {

            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database))
            {
                var tableSchema = new TableSchema();

                DocumentsOperationContext context;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                {
                    var tx1 = context.OpenWriteTransaction();
                    var b = context.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes("{}")), "adi");

                    database.DocumentsStorage.Put(context, "1", null, b);
                    database.DocumentsStorage.Put(context, "2", null, b);

                    var tx2 = tx1.BeginAsyncCommitAndStartNewTransaction(context);
                    context.Transaction = tx2;

                    database.DocumentsStorage.Put(context, "1", null, b);
                    database.DocumentsStorage.Put(context, "2", null, b);

                    tx1.EndAsyncCommit();
                    tx1.Dispose();

                    tx2.InnerTransaction.OpenTable(tableSchema, "Collection.Tombstones.@empty");
                    var tx3 = tx2.BeginAsyncCommitAndStartNewTransaction(context);
                    context.Transaction = tx3;

                    tx3.InnerTransaction.OpenTable(tableSchema, "Collection.Tombstones.@empty");
                    tx2.EndAsyncCommit();
                    tx2.Dispose();

                    tx3.Commit();
                    tx3.Dispose();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                {
                    var tx1 = context.OpenWriteTransaction();
                    var b = context.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes("{}")), "adi2");

                    database.DocumentsStorage.Put(context, "1", null, b);
                    database.DocumentsStorage.Put(context, "2", null, b);

                    tx1.Commit();
                }
            }
        }
    }
}
