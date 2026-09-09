using Raven.Server.Documents.Sharding;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas
{
    public static class Documents
    {
        public static readonly Slice CollectionEtagsSlice;

        internal static readonly Slice DocsSlice;
        internal static readonly Slice AllDocsEtagsSlice;
        internal static readonly Slice AllDocsBucketAndEtagSlice;

        internal static readonly TableSchema ShardingDocsSchemaBase = new TableSchema
        {
            TableType = (byte)TableType.Documents
        };

        internal static readonly TableSchema ShardingCompressedDocsSchemaBase = new TableSchema
        {
            TableType = (byte)TableType.Documents
        };

        internal static readonly TableSchema DocsSchemaBase = new TableSchema
        {
            TableType = (byte)TableType.Documents
        };

        internal static readonly TableSchema CompressedDocsSchemaBase = new TableSchema
        {
            TableType = (byte)TableType.Documents
        };

        public enum DocumentsTable
        {
            LowerId = 0,
            Etag = 1,
            Id = 2, // format of lazy string id is detailed in DocumentIdWorker.Compatibility.GetLowerIdSliceAndStorageKey
            Data = 3,
            ChangeVector = 4,
            LastModified = 5,
            Flags = 6,
            TransactionMarker = 7
        }

        internal static readonly TableSchema.FixedSizeKeyIndexDef CollectionEtagsIndex;
        internal static readonly TableSchema.FixedSizeKeyIndexDef AllDocsEtagsIndex;

        internal static readonly TableSchema.DynamicKeyIndexDef AllDocsBucketAndEtagIndex;

        static Documents()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Docs", ByteStringType.Immutable, out DocsSlice);
                Slice.From(ctx, "CollectionEtags", ByteStringType.Immutable, out CollectionEtagsSlice);
                Slice.From(ctx, "AllDocsEtags", ByteStringType.Immutable, out AllDocsEtagsSlice);
                Slice.From(ctx, "AllDocsBucketAndEtag", ByteStringType.Immutable, out AllDocsBucketAndEtagSlice);
            }

            CollectionEtagsIndex = new TableSchema.FixedSizeKeyIndexDef
            {
                StartIndex = (int)DocumentsTable.Etag,
                Name = CollectionEtagsSlice,
                IsGlobal = false
            };

            AllDocsEtagsIndex = new TableSchema.FixedSizeKeyIndexDef
            {
                StartIndex = (int)DocumentsTable.Etag,
                Name = AllDocsEtagsSlice,
                IsGlobal = true
            };
            AllDocsBucketAndEtagIndex = new TableSchema.DynamicKeyIndexDef
            {
                GenerateKey = ShardedDocumentsStorage.GenerateBucketAndEtagIndexKeyForDocuments,
                OnEntryChanged = ShardedDocumentsStorage.UpdateBucketStatsForDocument,
                IsGlobal = true,
                Name = AllDocsBucketAndEtagSlice
            };



            DefineIndexesForDocsSchemaBase(DocsSchemaBase);
            DefineIndexesForDocsSchemaBase(CompressedDocsSchemaBase);

            DocsSchemaBase.CompressValues(CollectionEtagsIndex, compress: false);
            CompressedDocsSchemaBase.CompressValues(CollectionEtagsIndex, compress: true);

            DefineIndexesForShardingDocsSchemaBase(ShardingDocsSchemaBase);
            DefineIndexesForShardingDocsSchemaBase(ShardingCompressedDocsSchemaBase);

            ShardingDocsSchemaBase.CompressValues(CollectionEtagsIndex, compress: false);
            ShardingCompressedDocsSchemaBase.CompressValues(CollectionEtagsIndex, compress: true);

            void DefineIndexesForDocsSchemaBase(TableSchema docsSchema)
            {
                docsSchema.DefineKey(new TableSchema.IndexDef
                {
                    StartIndex = (int)DocumentsTable.LowerId, 
                    Count = 1, 
                    IsGlobal = true, 
                    Name = DocsSlice
                });
                docsSchema.DefineFixedSizeIndex(CollectionEtagsIndex);
                docsSchema.DefineFixedSizeIndex(AllDocsEtagsIndex);
            }

            void DefineIndexesForShardingDocsSchemaBase(TableSchema docsSchema)
            {
                DefineIndexesForDocsSchemaBase(docsSchema);

                docsSchema.DefineIndex(AllDocsBucketAndEtagIndex);
            }
        }
    }

    public enum TableType : byte
    {
        None = 0,
        Documents = 1,
        Revisions = 2,
        Conflicts = 3,
        LegacyCounter = 4,
        Counters = 5,
        TimeSeries = 6
    }
}
