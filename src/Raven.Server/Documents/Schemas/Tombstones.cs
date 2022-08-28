using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas
{
    public static class Tombstones
    {
        public static TableSchema Current => TombstonesSchemaBase60;

        internal static readonly TableSchema TombstonesSchemaBase = new TableSchema();
        internal static readonly TableSchema TombstonesSchemaBase60 = new TableSchema();

        internal static readonly Slice TombstonesSlice;
        internal static readonly Slice AllTombstonesEtagsSlice;
        internal static readonly Slice TombstonesPrefix;
        internal static readonly Slice DeletedEtagsSlice;
        internal static readonly Slice TombstonesBucketAndEtagSlice;
        internal static readonly Slice CollectionTombstonesBucketAndEtagSlice;

        internal enum TombstoneTable
        {
            LowerId = 0,
            Etag = 1,
            DeletedEtag = 2,
            TransactionMarker = 3,
            Type = 4,
            Collection = 5,
            Flags = 6,
            ChangeVector = 7,
            LastModified = 8
        }

        static Tombstones()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "AllTombstonesEtags", ByteStringType.Immutable, out AllTombstonesEtagsSlice);
                Slice.From(ctx, "Tombstones", ByteStringType.Immutable, out TombstonesSlice);
                Slice.From(ctx, CollectionName.GetTablePrefix(CollectionTableType.Tombstones), ByteStringType.Immutable, out TombstonesPrefix);
                Slice.From(ctx, "DeletedEtags", ByteStringType.Immutable, out DeletedEtagsSlice);

                Slice.From(ctx, "TombstonesBucketAndEtag", ByteStringType.Immutable, out TombstonesBucketAndEtagSlice);
                Slice.From(ctx, "CollectionTombstonesBucketAndEtag", ByteStringType.Immutable, out CollectionTombstonesBucketAndEtagSlice);
            }

            DefineIndexesForTombstonesSchema(TombstonesSchemaBase);
            DefineIndexesForTombstonesSchemaBase60();

            void DefineIndexesForTombstonesSchema(TableSchema schema)
            {
                schema.DefineKey(new TableSchema.IndexDef
                {
                    StartIndex = (int)TombstoneTable.LowerId,
                    Count = 1,
                    IsGlobal = true,
                    Name = TombstonesSlice
                });
                schema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = (int)TombstoneTable.Etag,
                    IsGlobal = false,
                    Name = Documents.CollectionEtagsSlice
                });
                schema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = (int)TombstoneTable.Etag,
                    IsGlobal = true,
                    Name = AllTombstonesEtagsSlice
                });
                schema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = (int)TombstoneTable.DeletedEtag,
                    IsGlobal = false,
                    Name = DeletedEtagsSlice
                });
            }

            void DefineIndexesForTombstonesSchemaBase60()
            {
                DefineIndexesForTombstonesSchema(TombstonesSchemaBase60);

                TombstonesSchemaBase60.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = DocumentsStorage.GenerateBucketAndEtagIndexKeyForTombstones,
                    IsGlobal = true,
                    Name = TombstonesBucketAndEtagSlice
                });

                TombstonesSchemaBase60.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = DocumentsStorage.GenerateBucketAndEtagIndexKeyForTombstones,
                    IsGlobal = false,
                    Name = CollectionTombstonesBucketAndEtagSlice
                });
            }
        }
    }
}
