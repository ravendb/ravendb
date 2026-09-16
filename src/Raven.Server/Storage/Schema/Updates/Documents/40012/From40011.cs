using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Voron.Data.Tables;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Revisions.RevisionsStorage;
using static Raven.Server.Documents.Schemas.Revisions;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public sealed unsafe class From40011 : ISchemaUpdate
    {
        public int From => 40_011;
        public int To => 40_012;
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
        }

        public bool Update(UpdateStep step)
        {
            // When the revision are enabled and we delete a document we stored it with a 'RevisionDelete' flag. This flag was used to find the deleted revisions.
            // Now we store the resolved conflicts as revisions, so it could be, that a deleted revision will contain flags such as 'Conflicted' or 'Resolved'.

            // This change require use to change the index definition and the logic of how we find the deleted revisions.
            // So we say that if the revision is deleted it will be stored with the 'DeletedEtag' to the etag value, 
            // otherwise (if the revision is a document) it will be stored with 'DeletedEtag' set to 0.

            step.DocumentsStorage.RevisionsStorage = new RevisionsStorage(step.DocumentsStorage.DocumentDatabase, step.WriteTx, step.DocumentsStorage.RevisionsSchema, step.DocumentsStorage.CompressedRevisionsSchema);

            using (step.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                step.WriteTx.DeleteTree("RevisionsFlagsAndEtag"); // remove the old index
                step.WriteTx.CreateTree(DeleteRevisionEtagSlice);
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
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.ChangeVector, out int size), size);
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.LowerId, out size), size);
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.RecordSeparator, out size), size);
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.Etag, out size), size);
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.Id, out size), size);
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.Document, out size), size);
                            write.Add((int)flags);
                            if ((flags & DocumentFlags.DeleteRevision) == DocumentFlags.DeleteRevision)
                            {
                                write.Add(read.Reader.Read((int)LegacyRevisionsTable.Etag, out size), size); // set the DeletedEtag
                            }
                            else
                            {
                                write.Add(NotDeletedRevisionMarker);
                            }
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.LastModified, out size), size);
                            write.Add(read.Reader.Read((int)LegacyRevisionsTable.TransactionMarker, out size), size);
                            writeTable.Set(write, true);
                        }
                    }
                }
            }
            return true;
        }
    }
}
