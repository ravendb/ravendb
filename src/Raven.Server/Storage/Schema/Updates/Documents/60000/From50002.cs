using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using static Raven.Server.Documents.Schemas.Attachments;
using static Raven.Server.Documents.Schemas.Conflicts;
using static Raven.Server.Documents.Schemas.Counters;
using static Raven.Server.Documents.Schemas.DeletedRanges;
using static Raven.Server.Documents.Schemas.Documents;
using static Raven.Server.Documents.Schemas.Revisions;
using static Raven.Server.Documents.Schemas.TimeSeries;
using static Raven.Server.Documents.Schemas.Tombstones;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public class From50002 : ISchemaUpdate
    {
        public int From => 50_002;

        public int To => 60_000;
        
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        internal static int NumberOfItemsToMigrateInSingleTransaction = 10_000;
        internal static int MaxSizeToMigrateInSingleTransaction = 64 * 1024 * 1024;

        public bool Update(UpdateStep step)
        {
            InsertIndexValuesFor(step, DocsSchemaBase60, DocsBucketAndEtagSlice);
            InsertIndexValuesFor(step, TombstonesSchemaBase60, TombstonesBucketAndEtagSlice);
            InsertIndexValuesFor(step, RevisionsSchemaBase60, RevisionsBucketAndEtagSlice);
            InsertIndexValuesFor(step, TimeSeriesSchemaBase60, TimeSeriesBucketAndEtagSlice);
            InsertIndexValuesFor(step, DeleteRangesSchemaBase60, DeletedRangesBucketAndEtagSlice);
            InsertIndexValuesFor(step, CountersSchemaBase60, CountersBucketAndEtagSlice);

            InsertIndexValuesFor(step, ConflictsSchemaBase60, ConflictsBucketAndEtagSlice,
                fixedSizeIndex: ConflictsSchemaBase60.FixedSizeIndexes[AllConflictedDocsEtagsSlice]);

            InsertIndexValuesFor(step, AttachmentsSchemaBase60, AttachmentsBucketAndEtagSlice,
                tableName: AttachmentsMetadataSlice.ToString(),
                fixedSizeIndex: AttachmentsSchemaBase60.FixedSizeIndexes[AttachmentsEtagSlice]);

            return true;
        }

        public static void InsertIndexValuesFor(UpdateStep step, TableSchema schema, Slice indexName, string tableName = null, TableSchema.FixedSizeKeyIndexDef fixedSizeIndex = null)
        {
            var indexTree = step.WriteTx.CreateTree(indexName, isIndexTree: true);
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

                    var items = fixedSizeIndex == null
                        ? table.SeekByPrimaryKey(Slices.BeforeAllKeys, skip)
                        : table.SeekForwardFrom(fixedSizeIndex, 0, skip);

                    foreach (var tvh in items)
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
