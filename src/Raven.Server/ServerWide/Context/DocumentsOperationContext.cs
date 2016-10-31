using System;
using Raven.Server.Documents;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class DocumentsOperationContext : TransactionOperationContext<DocumentsTransaction>
    {
        private readonly DocumentDatabase _documentDatabase;

        public static DocumentsOperationContext ShortTermSingleUse(DocumentDatabase documentDatabase)
        {
            var shortTermSingleUse = new DocumentsOperationContext(documentDatabase, 4096, 1024);
            return shortTermSingleUse;
        }

        public DocumentsOperationContext(DocumentDatabase documentDatabase, int initialSize, int longLivedSize) :
            base(initialSize, longLivedSize)
        {
            _documentDatabase = documentDatabase;
        }

        protected override DocumentsTransaction CreateReadTransaction()
        {
            return new DocumentsTransaction(this, _documentDatabase.DocumentsStorage.Environment.ReadTransaction(PersistentContext, Allocator), _documentDatabase.Notifications);
        }

        protected override DocumentsTransaction CreateWriteTransaction()
        {
            var tx = new DocumentsTransaction(this, _documentDatabase.DocumentsStorage.Environment.WriteTransaction(PersistentContext, Allocator), _documentDatabase.Notifications);

            var options = _documentDatabase.DocumentsStorage.Environment.Options;

            if ((options.TransactionsMode == TransactionsMode.Lazy || options.TransactionsMode == TransactionsMode.Danger) &&
                options.NonSafeTransactionExpiration != null && options.NonSafeTransactionExpiration < DateTime.Now)
            {
                options.TransactionsMode = TransactionsMode.Safe;
            }

            tx.InnerTransaction.LowLevelTransaction.IsLazyTransaction = 
                options.TransactionsMode == TransactionsMode.Lazy;
            // IsLazyTransaction can be overriden later by a specific feature like bulk insert

            return tx;
        }

        public StorageEnvironment Environment => _documentDatabase.DocumentsStorage.Environment;

        public DocumentDatabase DocumentDatabase => _documentDatabase;

        public bool ShouldRenewTransactionsToAllowFlushing()
        {
            // if we have the same transaction id right now, there hasn't been write since we started the transaction
            // so there isn't really a major point in renewing the transaction, since we wouldn't be releasing any 
            // resources (scratch space, mostly) back to the system, let us continue with the current one.

            return Transaction?.InnerTransaction.LowLevelTransaction.Id !=
                   _documentDatabase.DocumentsStorage.Environment.CurrentReadTransactionId ;

        }
    }
}