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
    public unsafe class From40014 : ISchemaUpdate
    {
        public int From => 40_014;
        public int To => 40_015;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        public bool Update(UpdateStep step)
        {
            step.DocumentsStorage.RevisionsStorage = new RevisionsStorage(step.DocumentsStorage.DocumentDatabase, step.WriteTx);

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
                            var flags = TableValueToFlags((int)RevisionsTable.Flags, ref read.Reader);
                            var lastModified = TableValueToDateTime((int)RevisionsTable.LastModified, ref read.Reader);

                            write.Add(read.Reader.Read((int)RevisionsTable.ChangeVector, out int size), size);
                            write.Add(read.Reader.Read((int)RevisionsTable.LowerId, out size), size);
                            write.Add(read.Reader.Read((int)RevisionsTable.RecordSeparator, out size), size);
                            write.Add(read.Reader.Read((int)RevisionsTable.Etag, out size), size);
                            write.Add(read.Reader.Read((int)RevisionsTable.Id, out size), size);
                            write.Add(read.Reader.Read((int)RevisionsTable.Document, out size), size);
                            write.Add((int)flags);
                            write.Add(read.Reader.Read((int)RevisionsTable.DeletedEtag, out size), size);
                            write.Add(lastModified.Ticks);
                            write.Add(read.Reader.Read((int)RevisionsTable.TransactionMarker, out size), size);
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
