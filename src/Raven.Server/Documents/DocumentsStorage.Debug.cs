using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron.Data.Tables;

namespace Raven.Server.Documents
{
    public partial class DocumentsStorage
    {
        public Debugging ForDebug => new Debugging(this);

        public class Debugging
        {
            private readonly DocumentsStorage _storage;

            public Debugging(DocumentsStorage storage)
            {
                _storage = storage;
            }
            // Used to delete corrupted document from the JS admin console
            public bool DeleteDocumentByEtag(long etag)
            {
                using (_storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
                    var index = DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice];

                    if (table.FindByIndex(index, etag, out var reader) == false) 
                        return false;
                    
                    var doc = _storage.TableValueToDocument(context, ref reader, DocumentFields.LowerId);
                    _storage.Delete(context, doc.LowerId, DocumentFlags.None);
                    tx.Commit();
                    return true;
                }

                return false;
            }
            
            public bool DeleteRevisionByEtag(long etag, string collection = null)
            {
                using (_storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var index = Revisions.RevisionsStorage.RevisionsSchema.FixedSizeIndexes[Revisions.RevisionsStorage.AllRevisionsEtagsSlice];
                    if (collection == null)
                    {
                        using var table = new Table(Revisions.RevisionsStorage.RevisionsSchema, context.Transaction.InnerTransaction);
                        if (table.FindByIndex(index, etag, out var reader) == false)
                            return false;
                        
                        using var doc = Revisions.RevisionsStorage.TableValueToRevision(context, ref reader, DocumentFields.Data);
                        if (doc.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata)
                            && metadata.TryGet(Constants.Documents.Metadata.Collection, out collection) == false)
                            return false;
                    }
                    
                    var collectionName = new CollectionName(collection);
                    var tableName = collectionName.GetTableName(CollectionTableType.Revisions);
                    using var collectionTable = tx.InnerTransaction.OpenTable(Revisions.RevisionsStorage.RevisionsSchema, tableName);

                    if (collectionTable.DeleteByIndex(index, etag) == false) 
                        return false;
                    
                    tx.Commit();
                    return true;
                }
            }
        }
    }
}
