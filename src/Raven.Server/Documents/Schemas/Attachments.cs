using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas
{
    public static class Attachments
    {
        internal static readonly TableSchema AttachmentsSchemaBase = new TableSchema();
        internal static readonly TableSchema ShardingAttachmentsSchemaBase = new TableSchema();

        internal static readonly Slice AttachmentsSlice;
        internal static readonly Slice AttachmentsMetadataSlice;
        internal static readonly Slice AttachmentsEtagSlice;
        internal static readonly Slice AttachmentsHashSlice;
        internal static readonly Slice AttachmentsTombstonesSlice;
        internal static readonly Slice AttachmentsBucketAndEtagSlice;
        internal static readonly Slice AttachmentsBucketAndHashSlice;
        internal static readonly string AttachmentsTombstones = "Attachments.Tombstones";
        internal static readonly Slice AttachmentsHashAndFlagSlice;

        internal enum AttachmentsTable
        {
            /* AND is a record separator.
             * We are you using the record separator in order to avoid loading another files that has the same key prefix,
                e.g. fitz(record-separator)profile.png and fitz0(record-separator)profile.png, without the record separator we would have to load also fitz0 and filter it. */
            LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType = 0,
            Etag = 1,
            Name = 2, // format of lazy string key is detailed in GetLowerIdSliceAndStorageKey
            ContentType = 3, // format of lazy string key is detailed in GetLowerIdSliceAndStorageKey
            Hash = 4, // base64 hash
            TransactionMarker = 5,
            ChangeVector = 6,
            Size = 7,
            Flags = 8,
            RetireAt = 9
        }

        static Attachments()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Attachments", ByteStringType.Immutable, out AttachmentsSlice);
                Slice.From(ctx, "AttachmentsMetadata", ByteStringType.Immutable, out AttachmentsMetadataSlice);
                Slice.From(ctx, "AttachmentsEtag", ByteStringType.Immutable, out AttachmentsEtagSlice);
                Slice.From(ctx, "AttachmentsHash", ByteStringType.Immutable, out AttachmentsHashSlice);
                Slice.From(ctx, "AttachmentsBucketAndEtag", ByteStringType.Immutable, out AttachmentsBucketAndEtagSlice);
                Slice.From(ctx, "AttachmentsBucketAndHash", ByteStringType.Immutable, out AttachmentsBucketAndHashSlice);
                Slice.From(ctx, AttachmentsTombstones, ByteStringType.Immutable, out AttachmentsTombstonesSlice);
                Slice.From(ctx, "AttachmentsHashAndFlag", ByteStringType.Immutable, out AttachmentsHashAndFlagSlice);
            }

            DefineIndexesForAttachmentsSchema(AttachmentsSchemaBase);
            DefineIndexesForShardingAttachmentsSchema();


            void DefineIndexesForAttachmentsSchema(TableSchema schema)
            {
                schema.DefineKey(new TableSchema.IndexDef
                {
                    StartIndex = (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType,
                    Count = 1
                });
                schema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = (int)AttachmentsTable.Etag,
                    Name = AttachmentsEtagSlice
                });
                schema.DefineIndex(new TableSchema.IndexDef
                {
                    StartIndex = (int)AttachmentsTable.Hash,
                    Count = 1,
                    Name = AttachmentsHashSlice
                });

                schema.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = RetiredAttachmentsStorage.GenerateHashAndFlagForAttachments,
                //    OnEntryChanged = RetiredAttachmentsStorage.UpdateHashAndFlagForAttachments,
                    IsGlobal = true,
                    Name = AttachmentsHashAndFlagSlice,
                    SupportDuplicateKeys = true
                });
            }

            void DefineIndexesForShardingAttachmentsSchema()
            {
                DefineIndexesForAttachmentsSchema(ShardingAttachmentsSchemaBase);
                
                // the order here is important,
                // in the index 'AttachmentsBucketAndEtagSlice' we rely on the fact that we see the changes that were done during 'AttachmentsBucketAndHashSlice'

                ShardingAttachmentsSchemaBase.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = AttachmentsStorage.GenerateBucketAndHashForAttachments,
                    IsGlobal = true,
                    Name = AttachmentsBucketAndHashSlice,
                    SupportDuplicateKeys =  true
                });

                ShardingAttachmentsSchemaBase.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = AttachmentsStorage.GenerateBucketAndEtagIndexKeyForAttachments,
                    OnEntryChanged = AttachmentsStorage.UpdateBucketStatsForAttachments,
                    IsGlobal = true,
                    Name = AttachmentsBucketAndEtagSlice
                });
            }
        }
    }
}
