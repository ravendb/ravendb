using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas
{
    public static class CounterTombstones
    {
        public static TableSchema Current => CounterTombstonesSchemaBase;

        internal static readonly TableSchema CounterTombstonesSchemaBase = new TableSchema();

        internal static readonly Slice CounterTombstoneKey;
        internal static readonly Slice AllCounterTombstonesEtagSlice;
        internal static readonly Slice CollectionCounterTombstonesEtagsSlice;

        internal enum CounterTombstonesTable
        {
            // lower document id, record separator, lower counter name
            CounterTombstoneKey = 0,

            Etag = 1,
            ChangeVector = 2
        }

        static CounterTombstones()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "CounterTombstoneKey", ByteStringType.Immutable, out CounterTombstoneKey);
                Slice.From(ctx, "AllCounterTombstonesEtagSlice", ByteStringType.Immutable, out AllCounterTombstonesEtagSlice);
                Slice.From(ctx, "CollectionCounterTombstonesEtagsSlice", ByteStringType.Immutable, out CollectionCounterTombstonesEtagsSlice);
            }

            CounterTombstonesSchemaBase.DefineKey(new TableSchema.IndexDef
            {
                StartIndex = (int)CounterTombstonesTable.CounterTombstoneKey,
                Count = 1,
                Name = CounterTombstoneKey,
                IsGlobal = true
            });

            CounterTombstonesSchemaBase.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
            {
                StartIndex = (int)CounterTombstonesTable.Etag,
                Name = AllCounterTombstonesEtagSlice,
                IsGlobal = true
            });

            CounterTombstonesSchemaBase.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
            {
                StartIndex = (int)CounterTombstonesTable.Etag,
                Name = CollectionCounterTombstonesEtagsSlice
            });
        }
    }
}
