using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas
{
    public static class Attachments
    {
        public static TableSchema Current => AttachmentsSchemaBase60;

        internal static readonly TableSchema AttachmentsSchemaBase = new TableSchema();
        internal static readonly TableSchema AttachmentsSchemaBase60 = new TableSchema();

        internal static readonly Slice AttachmentsSlice;
        internal static readonly Slice AttachmentsMetadataSlice;
        internal static readonly Slice AttachmentsEtagSlice;
        internal static readonly Slice AttachmentsHashSlice;
        internal static readonly Slice AttachmentsTombstonesSlice;
        internal static readonly Slice AttachmentsBucketAndEtagSlice;
        internal static readonly string AttachmentsTombstones = "Attachments.Tombstones";

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
            ChangeVector = 6
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
                Slice.From(ctx, AttachmentsTombstones, ByteStringType.Immutable, out AttachmentsTombstonesSlice);
            }

            DefineIndexesForAttachmentsSchema(AttachmentsSchemaBase);
            DefineIndexesForAttachmentsSchemaBase60();

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
            }

            void DefineIndexesForAttachmentsSchemaBase60()
            {
                DefineIndexesForAttachmentsSchema(AttachmentsSchemaBase60);

                AttachmentsSchemaBase60.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = AttachmentsStorage.GenerateBucketAndEtagIndexKeyForAttachments,
                    IsGlobal = true,
                    Name = AttachmentsBucketAndEtagSlice
                });
            }
        }
    }
}
