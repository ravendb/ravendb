using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes
{
    public class LastDatabaseEtag
    {
        private const string LastDocumentEtagOnIndexCreationTreeName = "LastDocumentEtagOnIndexCreation";
        private const string Key = "LastEtag";

        public static long Get(TransactionContextPool contextPool)
        {
            using (contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            using (indexContext.OpenReadTransaction())
            {
                var tree = indexContext.Transaction.InnerTransaction.ReadTree(LastDocumentEtagOnIndexCreationTreeName);
                var result = tree?.Read(Key);
                return result?.Reader.ReadLittleEndianInt64() ?? 0;
            }
        }

        public static long Save(DocumentDatabase documentDatabase, Index index)
        {
            using (var queryContext = QueryOperationContext.Allocate(documentDatabase, index))
            using (queryContext.OpenReadTransaction())
            using (index._contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            using (var tx = indexContext.OpenWriteTransaction())
            using (Slice.From(indexContext.Allocator, Key, out var slice))
            {
                var lastDocumentEtag = DocumentsStorage.ReadLastEtag(queryContext.Documents.Transaction.InnerTransaction);
                var tree = indexContext.Transaction.InnerTransaction.CreateTree(LastDocumentEtagOnIndexCreationTreeName);
                tree.Add(slice, lastDocumentEtag);
                tx.Commit();

                return lastDocumentEtag;
            }
        }
    }
}
