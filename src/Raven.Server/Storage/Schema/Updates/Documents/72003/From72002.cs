using System;
using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Sparrow.Platform;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Schemas.Revisions;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public class From72002 : ISchemaUpdate
    {
        public int From => 72_002;
        public int To => 72_003;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        internal static int NumberOfRevisionsToScanInSingleTransaction = PlatformDetails.Is32Bits ? 2048 : 32768;

        public bool Update(UpdateStep step)
        {
            step.WriteTx.CreateTree(ConflictRevisionsSlice);

            foreach (var tableName in GetRevisionTableNames(step))
                ListConflictRevisions(step, tableName);

            return true;
        }

        private static List<string> GetRevisionTableNames(UpdateStep step)
        {
            var names = new List<string>();
            var prefix = CollectionName.GetTablePrefix(CollectionTableType.Revisions);

            using (var it = step.ReadTx.LowLevelTransaction.RootObjects.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    return names;

                do
                {
                    var name = it.CurrentKey.ToString();
                    if (name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    if (step.ReadTx.GetRootObjectType(it.CurrentKey) != RootObjectType.Table)
                        continue;

                    names.Add(name);
                } while (it.MoveNext());
            }

            return names;
        }

        private static void ListConflictRevisions(UpdateStep step, string tableName)
        {
            var skip = 0L;
            var done = false;

            while (done == false)
            {
                using (step.AllocateDocumentsOperationContext(out DocumentsOperationContext context))
                {
                    context.TransactionMarkerOffset = (short)step.WriteTx.LowLevelTransaction.Id;

                    var schema = ReadSchema(step, tableName);
                    var table = step.ReadTx.OpenTable(schema, tableName);
                    var processed = 0;
                    done = true;

                    if (table != null)
                    {
                        foreach (var read in table.SeekByPrimaryKey(Slices.BeforeAllKeys, skip))
                        {
                            var reader = read.Reader;
                            var flags = TableValueToFlags((int)RevisionsTable.Flags, ref reader);

                            if (flags.Contain(DocumentFlags.Conflicted) || flags.Contain(DocumentFlags.Resolved))
                            {
                                var etag = TableValueToEtag((int)RevisionsTable.Etag, ref reader);

                                using (TableValueToSlice(context, (int)RevisionsTable.LowerId, ref reader, out var lowerId))
                                using (RevisionsStorage.GetKeyPrefix(context.Allocator, lowerId, out var prefixSlice))
                                    RevisionsStorage.AddConflictRevision(context, step.WriteTx, prefixSlice, etag);
                            }

                            if (++processed >= NumberOfRevisionsToScanInSingleTransaction)
                            {
                                skip += processed;
                                done = false;
                                break;
                            }
                        }
                    }

                    step.Commit(context);
                    step.RenewTransactions();
                }
            }
        }

        private static unsafe TableSchema ReadSchema(UpdateStep step, string tableName)
        {
            var tableTree = step.ReadTx.ReadTree(tableName, RootObjectType.Table);
            var schemaSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
            var schemaPtr = tableTree.DirectRead(TableSchema.SchemasSlice);

            return TableSchema.ReadFrom(step.ReadTx.Allocator, schemaPtr, schemaSize);
        }
    }
}
