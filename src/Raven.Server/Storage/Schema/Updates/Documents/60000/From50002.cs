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

        private long _processedSinceLastCommit;

        public bool Update(UpdateStep step)
        {
            InsertIndexValuesFor(step, DocsSchemaBase60, DocsBucketAndEtagSlice,
                fixedSizeIndex: DocsSchemaBase60.FixedSizeIndexes[AllDocsEtagsSlice]);

            InsertIndexValuesFor(step, TombstonesSchemaBase60, TombstonesBucketAndEtagSlice,
                fixedSizeIndex: TombstonesSchemaBase60.FixedSizeIndexes[AllTombstonesEtagsSlice]);

            InsertIndexValuesFor(step, RevisionsSchemaBase60, RevisionsBucketAndEtagSlice,
                fixedSizeIndex: RevisionsSchemaBase60.FixedSizeIndexes[AllRevisionsEtagsSlice]);

            InsertIndexValuesFor(step, TimeSeriesSchemaBase60, TimeSeriesBucketAndEtagSlice,
                fixedSizeIndex: TimeSeriesSchemaBase60.FixedSizeIndexes[AllTimeSeriesEtagSlice]);

            InsertIndexValuesFor(step, DeleteRangesSchemaBase60, DeletedRangesBucketAndEtagSlice,
                fixedSizeIndex: DeleteRangesSchemaBase60.FixedSizeIndexes[AllDeletedRangesEtagSlice]);

            InsertIndexValuesFor(step, CountersSchemaBase60, CountersBucketAndEtagSlice,
                fixedSizeIndex: CountersSchemaBase60.FixedSizeIndexes[AllCountersEtagSlice]);

            InsertIndexValuesFor(step, ConflictsSchemaBase60, ConflictsBucketAndEtagSlice,
                fixedSizeIndex: ConflictsSchemaBase60.FixedSizeIndexes[AllConflictedDocsEtagsSlice]);

            InsertIndexValuesFor(step, AttachmentsSchemaBase60, AttachmentsBucketAndEtagSlice,
                fixedSizeIndex: AttachmentsSchemaBase60.FixedSizeIndexes[AttachmentsEtagSlice],
                tableName: AttachmentsMetadataSlice.ToString());

            return true;
        }

        private void InsertIndexValuesFor(UpdateStep step, TableSchema schema, Slice indexName, TableSchema.FixedSizeKeyIndexDef fixedSizeIndex, string tableName = null)
        {
            var fst = step.ReadTx.FixedTreeFor(fixedSizeIndex.Name, sizeof(long));
            if (fst.NumberOfEntries == 0)
                return;

            var indexTree = step.WriteTx.CreateTree(indexName, isIndexTree: true);
            var indexDef = schema.DynamicKeyIndexes[indexName];
            
            bool done = false;
            long lastEtag = 0;

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
                    var fromEtag = lastEtag == 0 ? 0 : lastEtag + 1;

                    foreach (var tvh in table.SeekForwardFrom(fixedSizeIndex, fromEtag, 0))
                    {
                        if (processedInCurrentTx >= NumberOfItemsToMigrateInSingleTransaction ||
                            _processedSinceLastCommit >= NumberOfItemsToMigrateInSingleTransaction)
                        {
                            commit = true;
                            break;
                        }

                        var value = tvh.Reader;
                        using (indexDef.GetValue(step.WriteTx.Allocator, ref value, out Slice val))
                        {
                            var index = new FixedSizeTree(step.WriteTx.LowLevelTransaction, indexTree, val, 0, isIndexTree: true);
                            index.Add(value.Id);
                        }

                        processedInCurrentTx++;
                        _processedSinceLastCommit++;
                    }

                    if (commit)
                    {
                        step.Commit(context);
                        step.RenewTransactions();

                        indexTree = step.WriteTx.ReadTree(indexName, isIndexTree: true);

                        lastEtag += processedInCurrentTx;
                        _processedSinceLastCommit = 0;

                        continue;
                    }

                    done = true;
                }
            }
        }
    }
}
