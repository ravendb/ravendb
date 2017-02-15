using System;
using Raven.Server.Files;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class FilesOperationContext : TransactionOperationContext<FilesTransaction>
    {
        private readonly FileSystem _fileSystem;

        public static FilesOperationContext ShortTermSingleUse(FileSystem fileSystem)
        {
            var shortTermSingleUse = new FilesOperationContext(fileSystem, 4096, 1024);
            return shortTermSingleUse;
        }

        public FilesOperationContext(FileSystem fileSystem, int initialSize, int longLivedSize) :
            base(initialSize, longLivedSize)
        {
            _fileSystem = fileSystem;
        }

        protected override FilesTransaction CreateReadTransaction()
        {
            return new FilesTransaction(this, _fileSystem.FilesStorage.Environment.ReadTransaction(PersistentContext, Allocator));
        }

        protected override FilesTransaction CreateWriteTransaction()
        {
            var tx = new FilesTransaction(this, _fileSystem.FilesStorage.Environment.WriteTransaction(PersistentContext, Allocator));

            CurrentTxMarker = (short) tx.InnerTransaction.LowLevelTransaction.Id;

            var options = _fileSystem.FilesStorage.Environment.Options;

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

        public StorageEnvironment Environment => _fileSystem.FilesStorage.Environment;

        public FileSystem FileSystem => _fileSystem;

        public bool ShouldRenewTransactionsToAllowFlushing()
        {
            // if we have the same transaction id right now, there hasn't been write since we started the transaction
            // so there isn't really a major point in renewing the transaction, since we wouldn't be releasing any 
            // resources (scratch space, mostly) back to the system, let us continue with the current one.

            return Transaction?.InnerTransaction.LowLevelTransaction.Id !=
                   _fileSystem.FilesStorage.Environment.CurrentReadTransactionId ;

        }
    }
}