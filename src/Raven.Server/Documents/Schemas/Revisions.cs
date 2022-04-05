using Raven.Server.Documents.Revisions;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas
{
    public static class Revisions
    {
        public static TableSchema Current => RevisionsSchemaBase60;

        public static TableSchema CurrentCompressed => CompressedRevisionsSchemaBase60;

        internal static readonly TableSchema RevisionsSchemaBase = new TableSchema()
        {
            TableType = (byte)TableType.Revisions,
        };

        internal static readonly TableSchema CompressedRevisionsSchemaBase = new TableSchema()
        {
            TableType = (byte)TableType.Revisions,
        };

        internal static readonly TableSchema RevisionsSchemaBase60 = new TableSchema()
        {
            TableType = (byte)TableType.Revisions,
        };

        internal static readonly TableSchema CompressedRevisionsSchemaBase60 = new TableSchema()
        {
            TableType = (byte)TableType.Revisions,
        };

        internal static readonly Slice IdAndEtagSlice;
        internal static readonly Slice DeleteRevisionEtagSlice;
        internal static readonly Slice AllRevisionsEtagsSlice;
        internal static readonly Slice CollectionRevisionsEtagsSlice;
        internal static readonly Slice RevisionsCountSlice;
        internal static readonly Slice RevisionsTombstonesSlice;
        internal static readonly Slice RevisionsPrefix;
        internal static Slice ResolvedFlagByEtagSlice;
        internal static readonly Slice RevisionsBucketAndEtagSlice;
        internal static readonly string RevisionsTombstones = "Revisions.Tombstones";

        public enum RevisionsTable
        {
            /* ChangeVector is the table's key as it's unique and will avoid conflicts (by replication) */
            ChangeVector = 0,
            LowerId = 1,
            /* We are you using the record separator in order to avoid loading another documents that has the same ID prefix,
                e.g. fitz(record-separator)01234567 and fitz0(record-separator)01234567, without the record separator we would have to load also fitz0 and filter it. */
            RecordSeparator = 2,
            Etag = 3, // etag to keep the insertion order
            Id = 4,
            Document = 5,
            Flags = 6,
            DeletedEtag = 7,
            LastModified = 8,
            TransactionMarker = 9,

            // Field for finding the resolved conflicts
            Resolved = 10,

            SwappedLastModified = 11,
        }

        static Revisions()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "RevisionsChangeVector", ByteStringType.Immutable, out var changeVectorSlice);
                Slice.From(ctx, "RevisionsIdAndEtag", ByteStringType.Immutable, out IdAndEtagSlice);
                Slice.From(ctx, "DeleteRevisionEtag", ByteStringType.Immutable, out DeleteRevisionEtagSlice);
                Slice.From(ctx, "AllRevisionsEtags", ByteStringType.Immutable, out AllRevisionsEtagsSlice);
                Slice.From(ctx, "CollectionRevisionsEtags", ByteStringType.Immutable, out CollectionRevisionsEtagsSlice);
                Slice.From(ctx, "RevisionsCount", ByteStringType.Immutable, out RevisionsCountSlice);
                Slice.From(ctx, "RevisionsBucketAndEtag", ByteStringType.Immutable, out RevisionsBucketAndEtagSlice);
                Slice.From(ctx, nameof(ResolvedFlagByEtagSlice), ByteStringType.Immutable, out ResolvedFlagByEtagSlice);
                Slice.From(ctx, RevisionsTombstones, ByteStringType.Immutable, out RevisionsTombstonesSlice);
                Slice.From(ctx, CollectionName.GetTablePrefix(CollectionTableType.Revisions), ByteStringType.Immutable, out RevisionsPrefix);


                DefineIndexesForRevisionsSchema(RevisionsSchemaBase, changeVectorSlice);
                DefineIndexesForRevisionsSchema(CompressedRevisionsSchemaBase, changeVectorSlice);

                DefineIndexesForRevisionsSchemaBase60(RevisionsSchemaBase60, changeVectorSlice);
                DefineIndexesForRevisionsSchemaBase60(CompressedRevisionsSchemaBase60, changeVectorSlice);

                RevisionsSchemaBase.CompressValues(
                    RevisionsSchemaBase.FixedSizeIndexes[CollectionRevisionsEtagsSlice], compress: false);
                CompressedRevisionsSchemaBase.CompressValues(
                    CompressedRevisionsSchemaBase.FixedSizeIndexes[CollectionRevisionsEtagsSlice], compress: true);

                RevisionsSchemaBase60.CompressValues(
                    RevisionsSchemaBase60.FixedSizeIndexes[CollectionRevisionsEtagsSlice], compress: false);
                CompressedRevisionsSchemaBase60.CompressValues(
                    CompressedRevisionsSchemaBase60.FixedSizeIndexes[CollectionRevisionsEtagsSlice], compress: true);

            }
        }

        private static void DefineIndexesForRevisionsSchema(TableSchema revisionsSchema, Slice changeVectorSlice)
        {
            revisionsSchema.DefineKey(new TableSchema.IndexDef
            {
                StartIndex = (int)RevisionsTable.ChangeVector,
                Count = 1,
                Name = changeVectorSlice,
                IsGlobal = true
            });
            revisionsSchema.DefineIndex(new TableSchema.IndexDef
            {
                StartIndex = (int)RevisionsTable.LowerId,
                Count = 3,
                Name = IdAndEtagSlice,
                IsGlobal = true
            });
            revisionsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
            {
                StartIndex = (int)RevisionsTable.Etag,
                Name = AllRevisionsEtagsSlice,
                IsGlobal = true
            });
            revisionsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
            {
                StartIndex = (int)RevisionsTable.Etag,
                Name = CollectionRevisionsEtagsSlice
            });
            revisionsSchema.DefineIndex(new TableSchema.IndexDef
            {
                StartIndex = (int)RevisionsTable.DeletedEtag,
                Count = 1,
                Name = DeleteRevisionEtagSlice,
                IsGlobal = true
            });
            revisionsSchema.DefineIndex(new TableSchema.IndexDef
            {
                StartIndex = (int)RevisionsTable.Resolved,
                Count = 2,
                Name = ResolvedFlagByEtagSlice,
                IsGlobal = true
            });
        }

        private static void DefineIndexesForRevisionsSchemaBase60(TableSchema schema, Slice changeVectorSlice)
        {
            DefineIndexesForRevisionsSchema(schema, changeVectorSlice);

            schema.DefineIndex(new TableSchema.DynamicKeyIndexDef
            {
                GenerateKey = RevisionsStorage.GenerateBucketAndEtagIndexKeyForRevisions,
                IsGlobal = true,
                Name = RevisionsBucketAndEtagSlice
            });
        }
    }
}
