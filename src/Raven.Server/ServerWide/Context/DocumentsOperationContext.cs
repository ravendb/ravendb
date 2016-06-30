using Raven.Server.Documents;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class DocumentsOperationContext : TransactionOperationContext<DocumentsTransaction>
    {
        private readonly DocumentDatabase _documentDatabase;
       
        public DocumentsOperationContext(UnmanagedBuffersPool pool, DocumentDatabase documentDatabase)
            : base(pool)
        {
            _documentDatabase = documentDatabase;
        }

        protected override DocumentsTransaction CreateReadTransaction(ByteStringContext context)
        {
            return new DocumentsTransaction(this, _documentDatabase.DocumentsStorage.Environment.ReadTransaction(context), _documentDatabase.Notifications);
        }

        protected override DocumentsTransaction CreateWriteTransaction(ByteStringContext context)
        {
            return new DocumentsTransaction(this, _documentDatabase.DocumentsStorage.Environment.WriteTransaction(context), _documentDatabase.Notifications);
        }

        public StorageEnvironment Environment()
        {
            return _documentDatabase.DocumentsStorage.Environment;
        }
    }
}