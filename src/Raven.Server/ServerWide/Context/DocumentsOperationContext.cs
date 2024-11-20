using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Linq;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Utils;
using Sparrow.Threading;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Context
{
    public sealed class DocumentsOperationContext : TransactionOperationContext<DocumentsTransaction>
    {
        private readonly DocumentDatabase _documentDatabase;

        private enum TableIndexes
        {
            Documents,
            Tombstones,
            Conflicts,
            Counters,
            Revisions,
            TimeSeries,
            DeleteRanges,
            CounterTombstones,
            TimeSeriesStats,
            Last
        }

        [InlineArray((int)TableIndexes.Last)]
        private struct Tables
        {
            public Table First;
        }
        
        private Tables _tables;
        

        public Table TimeSeriesStatsTable() =>
            _tables[(int)TableIndexes.TimeSeriesStats] ??= new Table(Documents.TimeSeries.TimeSeriesStats.TimeSeriesStatsSchema, Transaction.InnerTransaction);

        public Table DeleteRangesTable(TimeSeriesStorage timeSeries) =>
            _tables[(int)TableIndexes.DeleteRanges] ??= new Table(timeSeries.DeleteRangesSchema, Transaction.InnerTransaction);

        public Table TimesSeriesTable(TimeSeriesStorage timeSeries) =>
            _tables[(int)TableIndexes.TimeSeries] ??= new Table(timeSeries.TimeSeriesSchema, Transaction.InnerTransaction);

        public Table RevisionsTable(RevisionsStorage revisions, EventHandler<InvalidOperationException> onCorruptedDataHandler = null) =>
            _tables[(int)TableIndexes.Revisions] ??= new Table(revisions.RevisionsSchema, Transaction.InnerTransaction, onCorruptedDataHandler);
        
        public Table CountersTombstonesTable(CountersStorage counters) => 
            _tables[(int)TableIndexes.CounterTombstones] ??= new Table(counters.CounterTombstonesSchema, Transaction.InnerTransaction);

        public Table CountersTable(CountersStorage counters) => 
            _tables[(int)TableIndexes.Counters] ??= new Table(counters.CountersSchema, Transaction.InnerTransaction);
        public Table DocumentsTable(DocumentsStorage docs, EventHandler<InvalidOperationException> onCorruptedDataHandler = null) =>
            _tables[(int)TableIndexes.Documents] ??= new Table(docs.DocsSchema, Transaction.InnerTransaction, onCorruptedDataHandler);
        public Table TombstonesTable(DocumentsStorage docs) => 
            _tables[(int)TableIndexes.Tombstones] ??= new Table(docs.TombstonesSchema, Transaction.InnerTransaction);

        public Table ConflictsTable(ConflictsStorage conflicts) =>
            _tables[(int)TableIndexes.Conflicts] ??= new Table(conflicts.ConflictsSchema, Transaction.InnerTransaction);
        
        public DocumentsOperationContext(DocumentDatabase documentDatabase, int initialSize, int longLivedSize, int maxNumberOfAllocatedStringValues, SharedMultipleUseFlag lowMemoryFlag)
            : base(documentDatabase?.DocumentsStorage?.Environment, initialSize, longLivedSize, maxNumberOfAllocatedStringValues, lowMemoryFlag)
        {
            _documentDatabase = documentDatabase;
        }

        internal ChangeVector LastDatabaseChangeVector
        {
            get => _lastDatabaseChangeVector;
            set
            {
                if (value.IsSingle == false)
                    throw new InvalidOperationException($"The global change vector '{value}' cannot contain pipe");

                value = value.StripSinkTags(this);
                value = value.StripTrxnTags(this);

                if (DbIdsToIgnore == null || DbIdsToIgnore.Count == 0 || value.IsNullOrEmpty)
                {
                    _lastDatabaseChangeVector = value;
                    return;
                }

                value.TryRemoveIds(DbIdsToIgnore, this, out value);
                _lastDatabaseChangeVector = value;
            }
        }

        private ChangeVector _lastDatabaseChangeVector;
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

        public bool CanContinueTransaction
        {
            get
            {
                return Transaction.InnerTransaction.LowLevelTransaction.TransactionSize <= _documentDatabase._maxTransactionSize;
            }
        }

        protected internal override void Reset(bool forceResetLongLivedAllocator = false)
        {
            base.Reset(forceResetLongLivedAllocator);

            ResetTablesCache();
            
            // make sure that we don't remember an old value here from a previous
            // tx. This can be an issue if we resort to context stealing from 
            // other threads, so we are going the safe route and ensuring that 
            // we always create a new instance
            _lastDatabaseChangeVector = null;
            LastReplicationEtagFrom = null;
            DbIdsToIgnore = null;
            _skipChangeVectorValidation = false;
        }

        public void ResetTablesCache() => _tables = default;

        public static DocumentsOperationContext ShortTermSingleUse(DocumentDatabase documentDatabase)
        {
            var shortTermSingleUse = new DocumentsOperationContext(documentDatabase, 4096, 1024, 8 * 1024, SharedMultipleUseFlag.None);
            return shortTermSingleUse;
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

            return tx;
        }

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
