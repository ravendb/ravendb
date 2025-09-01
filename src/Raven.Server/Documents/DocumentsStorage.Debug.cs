using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;
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
                    
                    var doc = _storage.TableValueToDocument(context, ref reader, DocumentFields.LowerId | DocumentFields.Id);
                    bool isDelete;
                    using (Slice.From(context.Allocator, doc.LowerId.AsReadOnlySpan(), out var lowerId))
                    {
                        var result = _storage.Delete(context, lowerId, doc.Id, null);
                        isDelete = result.HasValue;
                    }
                    tx.Commit();
                    return isDelete;
                }
            }
        }
    }
}
