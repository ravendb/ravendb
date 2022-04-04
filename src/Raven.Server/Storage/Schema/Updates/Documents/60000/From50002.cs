using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;
using Voron.Data;
using Voron.Data.Fixed;
using Voron.Data.Tables;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public class From50002 : ISchemaUpdate
    {
        public int From => 50_002;

        public int To => 60_000;
        
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        internal static int NumberOfItemsToMigrateInSingleTransaction = 10_000;
        internal static int MaxSizeToMigrateInSingleTransaction = 64 * 1024 * 1024;

        internal static readonly Slice CountersBucketAndEtagSlice;
        internal static readonly Slice TombstonesBucketAndEtagSlice;

        private static readonly Slice DocsBucketAndEtagSlice;
        private static readonly Slice ConflictsBucketAndEtagSlice;
        private static readonly Slice RevisionsBucketAndEtagSlice;
        private static readonly Slice AttachmentsBucketAndEtagSlice;
        private static readonly Slice AttachmentsEtagSlice;
        private static readonly Slice TimeSeriesBucketAndEtagSlice;
        private static readonly Slice DeletedRangesBucketAndEtagSlice;
        private static readonly Slice AllConflictedDocsEtagsSlice;
        private static readonly Slice AllRevisionsEtagsSlice;
        private const string AttachmentsMetadata = "AttachmentsMetadata";


        static From50002()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "CountersBucketAndEtag", ByteStringType.Immutable, out CountersBucketAndEtagSlice);
                Slice.From(ctx, "DocsBucketAndEtag", ByteStringType.Immutable, out DocsBucketAndEtagSlice);
                Slice.From(ctx, "TombstonesBucketAndEtag", ByteStringType.Immutable, out TombstonesBucketAndEtagSlice);
                Slice.From(ctx, "ConflictsBucketAndEtag", ByteStringType.Immutable, out ConflictsBucketAndEtagSlice);
                Slice.From(ctx, "AllConflictedDocsEtags", ByteStringType.Immutable, out AllConflictedDocsEtagsSlice);
                Slice.From(ctx, "RevisionsBucketAndEtag", ByteStringType.Immutable, out RevisionsBucketAndEtagSlice);
                Slice.From(ctx, "AllRevisionsEtags", ByteStringType.Immutable, out AllRevisionsEtagsSlice);
                Slice.From(ctx, "AttachmentsBucketAndEtag", ByteStringType.Immutable, out AttachmentsBucketAndEtagSlice);
                Slice.From(ctx, "AttachmentsEtag", ByteStringType.Immutable, out AttachmentsEtagSlice);
                Slice.From(ctx, "TimeSeriesBucketAndEtag", ByteStringType.Immutable, out TimeSeriesBucketAndEtagSlice);
                Slice.From(ctx, "DeletedRangesBucketAndEtag", ByteStringType.Immutable, out DeletedRangesBucketAndEtagSlice);
            }
        }

        public bool Update(UpdateStep step)
        {
            InsertIndexValuesFor(step, DocumentsStorage.DocsSchema, DocsBucketAndEtagSlice);
            InsertIndexValuesFor(step, DocumentsStorage.TombstonesSchema, TombstonesBucketAndEtagSlice);
            InsertIndexValuesFor(step, RevisionsStorage.CompressedRevisionsSchema, RevisionsBucketAndEtagSlice);
            InsertIndexValuesFor(step, CountersStorage.CountersSchema, CountersBucketAndEtagSlice);
            InsertIndexValuesFor(step, TimeSeriesStorage.TimeSeriesSchema, TimeSeriesBucketAndEtagSlice);
            InsertIndexValuesFor(step, TimeSeriesStorage.DeleteRangesSchema, DeletedRangesBucketAndEtagSlice);

            InsertIndexValuesFor(step, ConflictsStorage.ConflictsSchema, ConflictsBucketAndEtagSlice,
                fixedSizeIndex: ConflictsStorage.ConflictsSchema.FixedSizeIndexes[AllConflictedDocsEtagsSlice]);

            InsertIndexValuesFor(step, AttachmentsStorage.AttachmentsSchema, AttachmentsBucketAndEtagSlice,
                tableName: AttachmentsMetadata,
                fixedSizeIndex: AttachmentsStorage.AttachmentsSchema.FixedSizeIndexes[AttachmentsEtagSlice]);

            return true;
        }


        public static void InsertIndexValuesFor(UpdateStep step, TableSchema schema, Slice indexName, string tableName = null, TableSchema.FixedSizeKeyIndexDef fixedSizeIndex = null)
        {
            var indexTree = step.WriteTx.ReadTree(indexName, RootObjectType.VariableSizeTree, isIndexTree: true);
            if (indexTree != null)
                return; // already processed by another schema-upgrade

            indexTree = step.WriteTx.CreateTree(indexName, isIndexTree: true);
            var indexDef = schema.DynamicKeyIndexes[indexName];
            
            bool done = false;
            long skip = 0;

            while (done == false)
            {
                var table = tableName == null
                    ? new Table(schema, step.ReadTx)
                    : step.ReadTx.OpenTable(schema, tableName);

                var processedInCurrentTx = 0;

                using (step.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    context.TransactionMarkerOffset = (short)step.WriteTx.LowLevelTransaction.Id;

                    var commit = false;

                    var enumerable = fixedSizeIndex == null
                        ? table.SeekByPrimaryKey(Slices.BeforeAllKeys, skip: skip)
                        : table.SeekForwardFrom(fixedSizeIndex, 0, skip);

                    foreach (var tvh in enumerable)
                    {
                        if (processedInCurrentTx >= NumberOfItemsToMigrateInSingleTransaction ||
                            context.AllocatedMemory >= MaxSizeToMigrateInSingleTransaction)
                        {
                            commit = true;
                            skip += processedInCurrentTx;
                            break;
                        }

                        var value = tvh.Reader;
                        using (indexDef.GetValue(step.WriteTx.Allocator, ref value, out Slice val))
                        {
                            var index = new FixedSizeTree(step.WriteTx.LowLevelTransaction, indexTree, val, 0, isIndexTree: true);
                            index.Add(value.Id);
                        }

                        processedInCurrentTx++;
                    }

                    if (commit)
                    {
                        step.Commit(context);
                        step.RenewTransactions();
                        continue;
                    }

                    done = true;
                }
            }
        }
    }
}
