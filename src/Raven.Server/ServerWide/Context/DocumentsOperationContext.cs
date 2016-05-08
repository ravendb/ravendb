using System;
using Raven.Server.Documents;
using Raven.Server.Json;
using Sparrow.Json;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class DocumentsOperationContext : TransactionOperationContext<DocumentsTransaction>
    {
        private readonly DocumentDatabase _documentDatabase;

        private bool _isLazyTransactionContext;

        public DocumentsOperationContext(UnmanagedBuffersPool pool, DocumentDatabase documentDatabase)
            : base(pool)
        {
            _documentDatabase = documentDatabase;
        }

        protected override DocumentsTransaction CreateReadTransaction()
        {
            return new DocumentsTransaction(this, _documentDatabase.DocumentsStorage.Environment.ReadTransaction(), _documentDatabase.Notifications);
        }

        protected override DocumentsTransaction CreateWriteTransaction()
        {
            return new DocumentsTransaction(this, _documentDatabase.DocumentsStorage.Environment.WriteTransaction(_isLazyTransactionContext), _documentDatabase.Notifications);
        }

        public LazyDocumentsOperation CreateLazyDocumentsOperation(DocumentsContextPool contextPool)
        {
            _isLazyTransactionContext = true;
            return new LazyDocumentsOperation(_documentDatabase.DocumentsStorage.Environment, contextPool);
        }
    }
}