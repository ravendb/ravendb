using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Voron.Data.Tables;
using static Raven.Server.Documents.Revisions.RevisionsStorage;
using static Raven.Server.Documents.DocumentsStorage;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public unsafe class From11 : ISchemaUpdate
    {
        public bool Update(UpdateStep step)
        {
            // When the revision are enabled and we delete a document we stored it with a 'RevisionDelete' flag. This flag was used to find the deleted revisions.
            // Now we store the resolved conflicts as revisions, so it could be, that a deleted revision will contain flags such as 'Conflicted' or 'Resolved'.

            // This change require use to change the index definition and the logic of how we find the deleted revisions.
            // So we say that if the revision is deleted it will be stored with the 'DeletedEtag' to the etag value, 
            // otherwise (if the revision is a document) it will be stored with 'DeletedEtag' set to 0.

            step.DocumentsStorage.RevisionsStorage = new RevisionsStorage(step.DocumentsStorage.DocumentDatabase, step.WriteTx);

            using (step.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                step.WriteTx.DeleteTree("RevisionsFlagsAndEtag"); // remove the old index

                foreach (var collection in step.DocumentsStorage.RevisionsStorage.GetCollections(step.ReadTx))
                {
                    var collectionName = new CollectionName(collection);
                    var readTable = step.DocumentsStorage.RevisionsStorage.EnsureRevisionTableCreated(step.ReadTx, collectionName);
                    if (readTable == null)
                        continue;
                    
                    var writeTable = step.DocumentsStorage.RevisionsStorage.EnsureRevisionTableCreated(step.WriteTx, collectionName);
                    foreach (var read in readTable.SeekForwardFrom(RevisionsSchema.FixedSizeIndexes[CollectionRevisionsEtagsSlice], 0, 0))
                    {
                        using (TableValueReaderUtil.CloneTableValueReader(context, read))
                        using (writeTable.Allocate(out TableValueBuilder write))
                        {
                            var flags = TableValueToFlags((int)Columns.Flags, ref read.Reader);
                            write.Add(read.Reader.Read((int)Columns.ChangeVector, out int size), size);
                            write.Add(read.Reader.Read((int)Columns.LowerId, out size), size);
                            write.Add(read.Reader.Read((int)Columns.RecordSeparator, out size), size);
                            write.Add(read.Reader.Read((int)Columns.Etag, out size), size);
                            write.Add(read.Reader.Read((int)Columns.Id, out size), size);
                            write.Add(read.Reader.Read((int)Columns.Document, out size), size);
                            write.Add((int)flags);
                            if ((flags & DocumentFlags.DeleteRevision) == DocumentFlags.DeleteRevision)
                            {
                                write.Add(read.Reader.Read((int)Columns.Etag, out size), size); // set the DeletedEtag
                            }
                            else
                            {
                                write.Add(NotDeletedRevisionMarker);
                            }
                            write.Add(read.Reader.Read((int)Columns.LastModified, out size), size);
                            write.Add(read.Reader.Read((int)Columns.TransactionMarker, out size), size);
                            writeTable.Set(write, true);
                        }
                    }
                }
            }
            return true;
        }
    }
}
