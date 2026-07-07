using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Voron.Data.Tables;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Schemas.Revisions;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public sealed unsafe class From40014 : ISchemaUpdate
    {
        public int From => 40_014;
        public int To => 40_015;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        public enum LegacyRevisionsTable
        {
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

        public bool Update(UpdateStep step)
        {
            step.DocumentsStorage.RevisionsStorage = new RevisionsStorage(step.DocumentsStorage.DocumentDatabase, step.WriteTx, step.DocumentsStorage.RevisionsSchema, step.DocumentsStorage.CompressedRevisionsSchema);

            // update revisions
            using (step.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                foreach (var collection in step.DocumentsStorage.RevisionsStorage.GetCollections(step.ReadTx))
                {
                    var collectionName = new CollectionName(collection);
                    var tableName = collectionName.GetTableName(CollectionTableType.Revisions);
                    var readTable = step.ReadTx.OpenTable(RevisionsSchemaBase, tableName);
                    if (readTable == null)
                        continue;

                    var writeTable = step.DocumentsStorage.RevisionsStorage.EnsureRevisionTableCreated(step.WriteTx, collectionName, RevisionsSchemaBase);
                    foreach (var read in readTable.SeekForwardFrom(RevisionsSchemaBase.FixedSizeIndexes[CollectionRevisionsEtagsSlice], 0, 0))
                    {
                        using (TableValueReaderUtil.CloneTableValueReader(context, read))
                        using (writeTable.Allocate(out TableValueBuilder write))
                        {
                            var flags = TableValueToFlags((int)LegacyRevisionsTable.Flags, ref read.Reader);
                            var lastModified = TableValueToDateTime((int)LegacyRevisionsTable.LastModified, ref read.Reader);

                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.ChangeVector, out int size), size);
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.LowerId, out size), size);
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.RecordSeparator, out size), size);
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.Etag, out size), size);
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.Id, out size), size);
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.Document, out size), size);
                            write.Add((int)flags);
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.DeletedEtag, out size), size);
                            write.Add(lastModified.Ticks);
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.TransactionMarker, out size), size);
                            if ((flags & DocumentFlags.Resolved) == DocumentFlags.Resolved)
                            {
                                write.Add((int)DocumentFlags.Resolved);
                            }
                            else
                            {
                                write.Add(0);
                            }
                            write.Add(Bits.SwapBytes(lastModified.Ticks));
                            writeTable.Set(write, true);
                        }
                    }
                }
            }

            return true;
        }
    }
}
