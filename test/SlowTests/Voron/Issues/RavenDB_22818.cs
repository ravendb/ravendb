using System.Linq;
using FastTests.Voron;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.RawData;
using Voron.Data.Tables;
using Voron.Global;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_22818 : StorageTest
{
    private static readonly TableSchema Schema;

    static RavenDB_22818()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "Key", ByteStringType.Immutable, out var keySlice);
            Schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef
                {
                    StartIndex = 0,
                    Count = 1
                });
        }
    }

    public RavenDB_22818(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void TableStorageReport_ShouldNotDoubleCountFreedOverflowPages()
    {
        // Large values (> MaxItemSize) are stored as overflow pages and _stats.OverflowPageCount
        // is incremented on insert. Before the fix, OverflowPageCount was never decremented on
        // delete, so TableReport.AllocatedSpaceInBytes included those freed pages even though
        // they were already counted in DataFile.FreeSpaceInBytes - causing the Storage Report
        // to show >100% at the Datafile level.

        const string tableName = "test";
        const int entryCount = 10;

        // A value exceeding MaxItemSize guarantees overflow page storage
        var largeValue = new byte[RawDataSection.MaxItemSize + 1];

        using (var tx = Env.WriteTransaction())
        {
            Schema.Create(tx, tableName, 16);
            tx.Commit();
        }

        var keys = new string[entryCount];
        using (var tx = Env.WriteTransaction())
        {
            var table = tx.OpenTable(Schema, tableName);
            for (int i = 0; i < entryCount; i++)
            {
                keys[i] = "key/" + i;
                Slice.From(tx.Allocator, keys[i], ByteStringType.Immutable, out var keySlice);
                fixed (byte* valuePtr = largeValue)
                {
                    var tvb = new TableValueBuilder
                    {
                        { keySlice.Content.Ptr, keySlice.Content.Length },
                        { valuePtr, largeValue.Length }
                    };
                    table.Set(tvb);
                }
            }
            tx.Commit();
        }

        long tableAllocatedAfterInsert;
        using (var tx = Env.ReadTransaction())
        {
            var report = Env.GenerateDetailedReport(tx, includeDetails: false);
            tableAllocatedAfterInsert = report.Tables.Single(t => t.Name == tableName).AllocatedSpaceInBytes;
        }

        // Delete all entries - this is the operation that exposed the bug:
        // overflow pages go to the free space handler but OverflowPageCount is not decremented.
        using (var tx = Env.WriteTransaction())
        {
            var table = tx.OpenTable(Schema, tableName);
            for (int i = 0; i < entryCount; i++)
            {
                Slice.From(tx.Allocator, keys[i], ByteStringType.Immutable, out var keySlice);
                table.DeleteByKey(keySlice);
            }
            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        {
            var report = Env.GenerateDetailedReport(tx, includeDetails: false);
            var tableReport = report.Tables.Single(t => t.Name == tableName);

            Assert.Equal(0, tableReport.NumberOfEntries);

            // After deleting all large (overflow) entries the table's AllocatedSpaceInBytes must
            // shrink by at least the space occupied by those overflow pages.
            // Before the fix, OverflowPageCount was never decremented, so AllocatedSpaceInBytes
            // remained the same after deletion - the freed pages were double-counted in both
            // DataFile.FreeSpaceInBytes and TableReport.AllocatedSpaceInBytes.
            var tableAllocatedAfterDelete = tableReport.AllocatedSpaceInBytes;
            var freedOverflowSpace = tableAllocatedAfterInsert - tableAllocatedAfterDelete;

            // Each of the entryCount large values occupies at least one overflow page (8 KB).
            var minExpectedFreed = (long)entryCount * Constants.Storage.PageSize;

            Assert.True(
                freedOverflowSpace >= minExpectedFreed,
                $"Expected table AllocatedSpaceInBytes to decrease by at least {minExpectedFreed:N0} bytes " +
                $"after deleting {entryCount} large entries, but it only decreased by {freedOverflowSpace:N0} bytes. " +
                $"Before deletion: {tableAllocatedAfterInsert:N0}, after deletion: {tableAllocatedAfterDelete:N0}.");
        }
    }
}
