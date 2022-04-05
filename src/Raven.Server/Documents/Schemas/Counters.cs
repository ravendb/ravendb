using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas
{
    public static class Counters
    {
        public static TableSchema Current => CountersSchemaBase60;

        internal static readonly TableSchema CountersSchemaBase = new TableSchema
        {
            TableType = (byte)TableType.Counters
        };

        internal static readonly TableSchema CountersSchemaBase60 = new TableSchema
        {
            TableType = (byte)TableType.Counters
        };

        internal static readonly Slice AllCountersEtagSlice;
        internal static readonly Slice CollectionCountersEtagsSlice;
        internal static readonly Slice CounterKeysSlice;
        internal static readonly Slice CountersBucketAndEtagSlice;

        internal enum CountersTable
        {
            // Format of this is:
            // lower document id, record separator, prefix
            CounterKey = 0,

            Etag = 1,
            ChangeVector = 2,
            Data = 3,
            Collection = 4,
            TransactionMarker = 5
        }

        static Counters()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "AllCounterGroupsEtags", ByteStringType.Immutable, out AllCountersEtagSlice);
                Slice.From(ctx, "CollectionCounterGroupsEtags", ByteStringType.Immutable, out CollectionCountersEtagsSlice);
                Slice.From(ctx, "CounterGroupKeys", ByteStringType.Immutable, out CounterKeysSlice);
                Slice.From(ctx, "CountersBucketAndEtag", ByteStringType.Immutable, out CountersBucketAndEtagSlice);
            }

            DefineIndexesForCountersSchema(CountersSchemaBase);
            DefineIndexesForCountersSchemaBase60();

            void DefineIndexesForCountersSchema(TableSchema schema)
            {
                schema.DefineKey(new TableSchema.IndexDef
                {
                    StartIndex = (int)CountersTable.CounterKey,
                    Count = 1,
                    Name = CounterKeysSlice,
                    IsGlobal = true,
                });

                schema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = (int)CountersTable.Etag,
                    Name = AllCountersEtagSlice,
                    IsGlobal = true
                });

                schema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = (int)CountersTable.Etag,
                    Name = CollectionCountersEtagsSlice
                });

            }

            void DefineIndexesForCountersSchemaBase60()
            {
                DefineIndexesForCountersSchema(CountersSchemaBase60);

                CountersSchemaBase60.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = CountersStorage.GenerateBucketAndEtagIndexKeyForCounters,
                    IsGlobal = true,
                    Name = CountersBucketAndEtagSlice
                });
            }
        }
    }
}
