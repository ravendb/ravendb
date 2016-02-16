using Raven.Server.Documents;
using Raven.Server.Json;

using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class DocumentsOperationContext : TransactionOperationContext
    {
        private readonly DocumentDatabase _documentDatabase;

        public new DocumentTransaction Transaction;

        public DocumentsOperationContext(UnmanagedBuffersPool pool, StorageEnvironment storageEnvironment)
            : base(pool, storageEnvironment)
        {
        }

        public DocumentsOperationContext(UnmanagedBuffersPool pool, DocumentDatabase documentDatabase)
            : base(pool, documentDatabase.DocumentsStorage.Environment)
        {
            _documentDatabase = documentDatabase;
        }

        public new DocumentTransaction OpenReadTransaction()
        {
            return Transaction = new DocumentTransaction(this, _documentDatabase.DocumentsStorage.Environment.ReadTransaction(), _documentDatabase.TasksStorage);
        }

        public new DocumentTransaction OpenWriteTransaction()
        {
            return Transaction = new DocumentTransaction(this, _documentDatabase.DocumentsStorage.Environment.WriteTransaction(), _documentDatabase.TasksStorage);
        }

        public override void Reset()
        {
            base.Reset();

            Transaction?.Dispose();
            Transaction = null;
        }
    }
}