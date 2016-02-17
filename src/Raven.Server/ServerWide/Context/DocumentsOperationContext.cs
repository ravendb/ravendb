using Raven.Server.Documents;
using Raven.Server.Json;

namespace Raven.Server.ServerWide.Context
{
    public class DocumentsOperationContext : TransactionOperationContext<DocumentTransaction>
    {
        private readonly DocumentDatabase _documentDatabase;

        public DocumentsOperationContext(UnmanagedBuffersPool pool, DocumentDatabase documentDatabase)
            : base(pool)
        {
            _documentDatabase = documentDatabase;
        }

        protected override DocumentTransaction CreateReadTransaction()
        {
            return new DocumentTransaction(this, _documentDatabase.DocumentsStorage.Environment.ReadTransaction(), _documentDatabase.TasksStorage);
        }

        protected override DocumentTransaction CreateWriteTransaction()
        {
            return new DocumentTransaction(this, _documentDatabase.DocumentsStorage.Environment.WriteTransaction(), _documentDatabase.TasksStorage);
        }
    }
}