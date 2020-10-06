using System;
using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Sparrow.Threading;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class DocumentsOperationContext : TransactionOperationContext<DocumentsTransaction>
    {
        private readonly DocumentDatabase _documentDatabase;

        internal string LastDatabaseChangeVector
        {
            get => _lastDatabaseChangeVector;
            set
            {
                if (DbIdsToIgnore == null || DbIdsToIgnore.Count == 0 || string.IsNullOrEmpty(value))
                {
                    _lastDatabaseChangeVector = value;
                    return;
                }
                
                var list = value.ToChangeVectorList();
                if (list.RemoveAll(x => DbIdsToIgnore.Contains(x.DbId)) > 0)
                {
                    _lastDatabaseChangeVector = list.SerializeVector();
                    return;
                }

                _lastDatabaseChangeVector = value;
            }
        }

        private string _lastDatabaseChangeVector;
        internal Dictionary<string, long> LastReplicationEtagFrom;
        private bool _skipChangeVectorValidation;
        internal HashSet<string> DbIdsToIgnore;

        internal bool SkipChangeVectorValidation
        {
            get => _skipChangeVectorValidation;
            set
            {
                if (_skipChangeVectorValidation == false)
                    _skipChangeVectorValidation = value;
            }
        }

        protected internal override void Reset(bool forceResetLongLivedAllocator = false)
        {
            base.Reset(forceResetLongLivedAllocator);

            // make sure that we don't remember an old value here from a previous
            // tx. This can be an issue if we resort to context stealing from 
            // other threads, so we are going the safe route and ensuring that 
            // we always create a new instance
            _lastDatabaseChangeVector = null;
            LastReplicationEtagFrom = null;
            DbIdsToIgnore = null;
            _skipChangeVectorValidation = false;
        }

        public static DocumentsOperationContext ShortTermSingleUse(DocumentDatabase documentDatabase)
        {
            var shortTermSingleUse = new DocumentsOperationContext(documentDatabase, 4096, 1024, 8 * 1024, SharedMultipleUseFlag.None);
            return shortTermSingleUse;
        }

        public DocumentsOperationContext(DocumentDatabase documentDatabase, int initialSize, int longLivedSize, int maxNumberOfAllocatedStringValues, SharedMultipleUseFlag lowMemoryFlag)
            : base(initialSize, longLivedSize, maxNumberOfAllocatedStringValues, lowMemoryFlag)
        {
            _documentDatabase = documentDatabase;
        }

        protected override DocumentsTransaction CloneReadTransaction(DocumentsTransaction previous)
        {
            var clonedTransaction = new DocumentsTransaction(this,
                _documentDatabase.DocumentsStorage.Environment.CloneReadTransaction(previous.InnerTransaction, PersistentContext, Allocator),
                _documentDatabase.Changes);

            previous.Dispose();

            return clonedTransaction;
        }

        protected override DocumentsTransaction CreateReadTransaction()
        {
            return new DocumentsTransaction(this,
                _documentDatabase.DocumentsStorage.Environment.ReadTransaction(PersistentContext, Allocator),
                _documentDatabase.Changes);
        }

        protected override DocumentsTransaction CreateWriteTransaction(TimeSpan? timeout = null)
        {
            var tx = new DocumentsTransaction(this,
                _documentDatabase.DocumentsStorage.Environment.WriteTransaction(PersistentContext, Allocator, timeout),
                _documentDatabase.Changes);

            CurrentTxMarker = (short)tx.InnerTransaction.LowLevelTransaction.Id;

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
                   _documentDatabase.DocumentsStorage.Environment.CurrentReadTransactionId;
        }
    }
}
