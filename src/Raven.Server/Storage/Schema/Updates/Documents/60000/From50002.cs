using System.Collections.Generic;
using Google.Protobuf.WellKnownTypes;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Utils;
using Voron.Data;
using Voron.Data.BTrees;
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
    public unsafe class From50002 : ISchemaUpdate
    {
        public int From => 50_002;

        public int To => 60_000;
        
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        internal static int NumberOfItemsToMigrateInSingleTransaction = 10_000;

        private long _processedInCurrentTx;

        public bool Update(UpdateStep step)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal, "what if we migrate from a compressed database?");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal, "test that we can get by bucket after schema upgrade");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal, "test on large dataset");

            Dictionary<string, CollectionName> collections;
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                collections = DocumentsStorage.ReadCollections(step.ReadTx, ctx);
            }

            InsertIndexValuesFor(step, DocsSchemaBase60, AllDocsBucketAndEtagSlice,
                fixedSizeIndex: DocsSchemaBase60.FixedSizeIndexes[AllDocsEtagsSlice],
                etagPosition: (int)DocumentsTable.Etag);

            foreach (var collection in collections)
            {
                InsertIndexValuesFor(step, DocsSchemaBase60, CollectionDocsBucketAndEtagSlice,
                    fixedSizeIndex: DocsSchemaBase60.FixedSizeIndexes[CollectionEtagsSlice],
                    etagPosition: (int)DocumentsTable.Etag,
                    collection.Value.GetTableName(CollectionTableType.Documents));
            }

            InsertIndexValuesFor(step, TombstonesSchemaBase60, TombstonesBucketAndEtagSlice,
                fixedSizeIndex: TombstonesSchemaBase60.FixedSizeIndexes[AllTombstonesEtagsSlice],
                etagPosition: (int)TombstoneTable.Etag);

            foreach (var collection in collections)
            {
                InsertIndexValuesFor(step, TombstonesSchemaBase60, CollectionTombstonesBucketAndEtagSlice,
                    fixedSizeIndex: TombstonesSchemaBase60.FixedSizeIndexes[CollectionEtagsSlice],
                    etagPosition: (int)TombstoneTable.Etag,
                    tableName: collection.Value.GetTableName(CollectionTableType.Tombstones));
            }

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
            var indexDef = schema.DynamicKeyIndexes[indexName];
            var table = tableName == null
                ? new Table(schema, step.WriteTx)
                : step.WriteTx.OpenTable(schema, tableName);

            var indexTree = CreateIndexTree(step, tableName, indexDef);

            bool done = false;
            long fromEtag = 0;

            while (done == false)
            {
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
                            var index = table.GetFixedSizeTree(indexTree, val, 0, indexDef.IsGlobal, isIndexTree: true);
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

        private static Tree CreateIndexTree(UpdateStep step, string tableName, TableSchema.DynamicKeyIndexDef indexDef)
        {
            if (indexDef.IsGlobal)
                return step.WriteTx.CreateTree(indexDef.Name, isIndexTree: true);

            // we are adding new index to an existing table
            var tableTree = step.WriteTx.ReadTree(tableName, RootObjectType.Table);
            var tablePageAllocator = new NewPageAllocator(step.WriteTx.LowLevelTransaction, tableTree);
            var indexTree = Tree.Create(step.WriteTx.LowLevelTransaction, step.WriteTx, indexDef.Name, isIndexTree: true, newPageAllocator: tablePageAllocator);
            using (tableTree.DirectAdd(indexDef.Name, sizeof(TreeRootHeader), out var ptr))
            {
                indexTree.State.CopyTo((TreeRootHeader*)ptr);
            }

            return indexTree;

        }
    }
}
