using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Sparrow.Binary;
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

        private long _processedInCurrentTx;

        public bool Update(UpdateStep step)
        {
            InsertIndexValuesFor(step, DocsSchemaBase60, AllDocsBucketAndEtagSlice,
                fixedSizeIndex: DocsSchemaBase60.FixedSizeIndexes[AllDocsEtagsSlice],
                etagPosition: (int)DocumentsTable.Etag);

            InsertIndexValuesFor(step, TombstonesSchemaBase60, TombstonesBucketAndEtagSlice,
                fixedSizeIndex: TombstonesSchemaBase60.FixedSizeIndexes[AllTombstonesEtagsSlice],
                etagPosition: (int)TombstoneTable.Etag);

            InsertIndexValuesFor(step, RevisionsSchemaBase60, RevisionsBucketAndEtagSlice,
                fixedSizeIndex: RevisionsSchemaBase60.FixedSizeIndexes[AllRevisionsEtagsSlice],
                etagPosition: (int)RevisionsTable.Etag);

            InsertIndexValuesFor(step, TimeSeriesSchemaBase60, TimeSeriesBucketAndEtagSlice,
                fixedSizeIndex: TimeSeriesSchemaBase60.FixedSizeIndexes[AllTimeSeriesEtagSlice],
                etagPosition: (int)TimeSeriesTable.Etag);

            InsertIndexValuesFor(step, DeleteRangesSchemaBase60, DeletedRangesBucketAndEtagSlice,
                fixedSizeIndex: DeleteRangesSchemaBase60.FixedSizeIndexes[AllDeletedRangesEtagSlice],
                etagPosition: (int)DeletedRangeTable.Etag);

            InsertIndexValuesFor(step, CountersSchemaBase60, CountersBucketAndEtagSlice,
                fixedSizeIndex: CountersSchemaBase60.FixedSizeIndexes[AllCountersEtagSlice],
                etagPosition: (int)CountersTable.Etag);

            InsertIndexValuesFor(step, ConflictsSchemaBase60, ConflictsBucketAndEtagSlice,
                fixedSizeIndex: ConflictsSchemaBase60.FixedSizeIndexes[AllConflictedDocsEtagsSlice],
                etagPosition: (int)ConflictsTable.Etag);

            InsertIndexValuesFor(step, AttachmentsSchemaBase60, AttachmentsBucketAndEtagSlice,
                fixedSizeIndex: AttachmentsSchemaBase60.FixedSizeIndexes[AttachmentsEtagSlice],
                etagPosition: (int)AttachmentsTable.Etag,
                tableName: AttachmentsMetadataSlice.ToString());

            return true;
        }

        private void InsertIndexValuesFor(UpdateStep step, TableSchema schema, Slice indexName, TableSchema.FixedSizeKeyIndexDef fixedSizeIndex, int etagPosition, string tableName = null)
        {
            var indexTree = step.WriteTx.CreateTree(indexName, isIndexTree: true);
            var indexDef = schema.DynamicKeyIndexes[indexName];
            
            bool done = false;
            long fromEtag = 0;

            while (done == false)
            {
                var table = tableName == null
                    ? new Table(schema, step.ReadTx)
                    : step.ReadTx.OpenTable(schema, tableName);

                using (step.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    context.TransactionMarkerOffset = (short)step.WriteTx.LowLevelTransaction.Id;

                    var commit = false;

                    foreach (var tvh in table.SeekForwardFrom(fixedSizeIndex, fromEtag, 0))
                    {
                        var value = tvh.Reader;

                        if (_processedInCurrentTx >= NumberOfItemsToMigrateInSingleTransaction)
                        {
                            fromEtag = DocumentsStorage.TableValueToEtag(etagPosition, ref value);
                            commit = true;
                            break;
                        }

                        using (indexDef.GetValue(step.WriteTx.Allocator, ref value, out Slice val))
                        {
                            var index = new FixedSizeTree(step.WriteTx.LowLevelTransaction, indexTree, val, 0, isIndexTree: true);
                            index.Add(value.Id);
                        }

                        _processedInCurrentTx++;
                    }

                    if (commit)
                    {
                        step.Commit(context);
                        step.RenewTransactions();

                        indexTree = step.WriteTx.ReadTree(indexName, isIndexTree: true);
                        _processedInCurrentTx = 0;

                        continue;
                    }

                    done = true;
                }
            }
        }
    }
}
