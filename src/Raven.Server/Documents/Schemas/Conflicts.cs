using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas
{
    public static class Conflicts
    {
        public static TableSchema Current => ConflictsSchemaBase60;

        internal static readonly TableSchema ConflictsSchemaBase = new TableSchema()
        {
            TableType = (byte)TableType.Conflicts
        };
        internal static readonly TableSchema ConflictsSchemaBase60 = new TableSchema()
        {
            TableType = (byte)TableType.Conflicts
        };

        internal static readonly Slice ChangeVectorSlice;
        internal static readonly Slice IdAndChangeVectorSlice;
        internal static readonly Slice AllConflictedDocsEtagsSlice;
        internal static readonly Slice ConflictedCollectionSlice;
        internal static readonly Slice ConflictsSlice;
        internal static readonly Slice ConflictsIdSlice;
        internal static readonly Slice ConflictsBucketAndEtagSlice;

        public enum ConflictsTable
        {
            LowerId = 0,
            RecordSeparator = 1,
            ChangeVector = 2,
            Id = 3,
            Data = 4,
            Etag = 5,
            Collection = 6,
            LastModified = 7,
            Flags = 8
        }

        static Conflicts()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "ChangeVector", ByteStringType.Immutable, out ChangeVectorSlice);
                Slice.From(ctx, "ConflictsId", ByteStringType.Immutable, out ConflictsIdSlice);
                Slice.From(ctx, "IdAndChangeVector", ByteStringType.Immutable, out IdAndChangeVectorSlice);
                Slice.From(ctx, "AllConflictedDocsEtags", ByteStringType.Immutable, out AllConflictedDocsEtagsSlice);
                Slice.From(ctx, "ConflictedCollection", ByteStringType.Immutable, out ConflictedCollectionSlice);
                Slice.From(ctx, "Conflicts", ByteStringType.Immutable, out ConflictsSlice);
                Slice.From(ctx, "ConflictsBucketAndEtag", ByteStringType.Immutable, out ConflictsBucketAndEtagSlice);
            }

            DefineIndexesForConflictsSchema(ConflictsSchemaBase);
            DefineIndexesForConflictsSchemaBase60();

            void DefineIndexesForConflictsSchema(TableSchema schema)
            {
                /*
 The structure of conflicts table starts with the following fields:
 [ Conflicted Doc Id | Separator | Change Vector | ... the rest of fields ... ]
 PK of the conflicts table will be 'Change Vector' field, because when dealing with conflicts,
  the change vectors will always be different, hence the uniqueness of the ID. (inserts/updates will not overwrite)

Additional index is set to have composite ID of 'Conflicted Doc Id' and 'Change Vector' so we will be able to iterate
on conflicts by conflicted doc id (using 'starts with')

We need a separator in order to delete all conflicts all "users/1" without deleting "users/11" conflicts.
 */

                schema.DefineKey(new TableSchema.IndexDef
                {
                    StartIndex = (int)ConflictsTable.ChangeVector,
                    Count = 1,
                    IsGlobal = false,
                    Name = ChangeVectorSlice
                });
                // required to get conflicts by ID
                schema.DefineIndex(new TableSchema.IndexDef
                {
                    StartIndex = (int)ConflictsTable.LowerId,
                    Count = 3,
                    IsGlobal = false,
                    Name = IdAndChangeVectorSlice
                });
                schema.DefineIndex(new TableSchema.IndexDef
                {
                    StartIndex = (int)ConflictsTable.LowerId,
                    Count = 1,
                    IsGlobal = true,
                    Name = ConflictsIdSlice
                });
                schema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = (int)ConflictsTable.Etag,
                    IsGlobal = true,
                    Name = AllConflictedDocsEtagsSlice
                });
                schema.DefineIndex(new TableSchema.IndexDef
                {
                    StartIndex = (int)ConflictsTable.Collection,
                    Count = 1,
                    IsGlobal = true,
                    Name = ConflictedCollectionSlice
                });
            }

            void DefineIndexesForConflictsSchemaBase60()
            {
                DefineIndexesForConflictsSchema(ConflictsSchemaBase60);

                ConflictsSchemaBase60.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = ConflictsStorage.GenerateBucketAndEtagIndexKeyForConflicts,
                    IsGlobal = true,
                    Name = ConflictsBucketAndEtagSlice
                });
            }
        }
    }
}
