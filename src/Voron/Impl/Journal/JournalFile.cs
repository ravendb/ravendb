using Sparrow;
using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow.Server.Platform;
using Sparrow.Threading;
using Constants = Voron.Global.Constants;

namespace Voron.Impl.Journal
{
    public sealed unsafe class JournalFile(StorageEnvironment env, JournalWriter journalWriter, long journalNumber, FrozenSet<Guid> recoveredJournalIds) : IDisposable
    {
        public long LastTransactionId;

        public readonly Dictionary<StorageEnvironment, JournalFile> RegisteredEnvironments = new();

        private List<TransactionHeader> _transactionHeaders = new();

        public FrozenSet<Guid> RecoveredJournalIds = recoveredJournalIds;

        // JournalId XORed with this, invalidates entries left over from a  recycled file.
        public Guid Incarnation;

        // 1 for a newly created file - the first write goes past the journal header record at position 0
        public long InitialWritePosIn4Kb;

        // the half-fill pool-preparation trigger fires once per file; merger thread only
        public bool PrewarmChecked;

        public bool IsHardLinked { get; init; }

        public override string ToString()
        {
            return $"Number: {Number}";
        }

        internal long GetWritePosIn4KbPosition(EnvironmentStateRecord record) => record.Journal.Number == Number ? record.Journal.Last4KWritePosition : InitialWritePosIn4Kb;

        public long Number { get; } = journalNumber;

        public SingleUseFlag DoneWriting;
        private JournalWriter _journalWriter = journalWriter;
        public bool NewlyCreatedFile;

        public long GetAvailable4Kbs(EnvironmentStateRecord record) => (_journalWriter?.NumberOfAllocated4Kb - GetWritePosIn4KbPosition(record)) ?? 0;

        public Size JournalSize => new Size(_journalWriter?.NumberOfAllocated4Kb * 4 ?? 0, SizeUnit.Kilobytes);

        internal JournalWriter JournalWriter => _journalWriter;

        public void Release()
        {
            if (_journalWriter?.Release() != true)
                return;

            Dispose();
        }

        public void AddRef()
        {
            _journalWriter?.AddRef();
        }

        public void Dispose()
        {
            _transactionHeaders = null;
            _journalWriter = null;
        }

        public (long FirstTransactionId, long LastTransactionId, int Count)  GetTransactionStatsFor(Guid journalId)
        {
            int count = 0;
            long first = long.MaxValue;
            long last = -1;
            for (int i = 0; i < _transactionHeaders.Count; i++)
            {
                if(_transactionHeaders[i].JournalId != journalId)
                    continue;
                count++;
                
                if (first == long.MaxValue) 
                    first = _transactionHeaders[i].TransactionId;
                
                last = _transactionHeaders[i].TransactionId;
            }

            return (first, last, count);
        }


        public TransactionHeader GetLastReadTxHeader(long maxTransactionId, Guid journalId)
        {
            long lastSeenTxId = long.MaxValue;

            // we have to scan here, since we get transactions from multiple environments
            for (int i = _transactionHeaders.Count - 1; i >= 0; i--)
            {
                var header = _transactionHeaders[i];

                // an empty journal id is a pre-8.0 transaction, from before journals could be shared, so it
                // can only belong to the environment that owns this file
                if (header.JournalId != journalId && header.JournalId != Guid.Empty)
                    continue;

                Debug.Assert(header.TransactionId < lastSeenTxId,
                    $"Transactions of a single journal id are appended in order, but {header.TransactionId} came before {lastSeenTxId}");
                lastSeenTxId = header.TransactionId;

                if (header.TransactionId <= maxTransactionId)
                    return header;
            }

            return new TransactionHeader { TransactionId = -1 };
        }

        /// <summary>
        /// Write a buffer of transactions (from lazy, usually) to the file
        /// </summary>
        public long Write(long posBy4Kb, Span<Pal.journal_entry> entries, SafeJournalWriteContext context)
        {
            Debug.Assert(DoneWriting is null || DoneWriting.IsRaised() == false, $"Journal {Number} was written after DoneWriting was raised.");

            long totalNumberOf4Kbs = 0;
            for (int i = 0; i < entries.Length; i++)
            {
                var readTxHeader = (TransactionHeader*)entries[i].Base;
                totalNumberOf4Kbs += entries[i].NumberOf4Kbs;
                Debug.Assert(readTxHeader->HeaderMarker == Constants.TransactionHeaderMarker);
            }

            JournalWriter.Write(posBy4Kb, entries, totalNumberOf4Kbs, context);
            
            return totalNumberOf4Kbs;
        }

        public void InitFrom(StorageEnvironment storageEnvironment, JournalReader journalReader, List<TransactionHeader> transactionHeaders)
        {
            storageEnvironment.UpdateJournal(Number, journalReader.Next4Kb);
            _transactionHeaders = [.. transactionHeaders];
        }

        public bool ShouldDelete
        {
            set
            {
                var writer = _journalWriter;

                if (writer != null)
                    writer.ShouldDelete = value;
            }
        }

        public bool HasLegacyTransaction
        {
            get
            {
                foreach (var tx in _transactionHeaders)
                {
                    // if this journal contains any transaction with empty database 
                    // then we cannot use it for current writes, since it may be a 
                    // root environment and confuse any branch env reading from it
                    if (tx.JournalId == Guid.Empty)
                        return true;
                }

                return false;
            }
        }

        public void SetTransactionFrom(Pal.journal_entry journalEntry)
        {
            _transactionHeaders.Add(*(TransactionHeader*)journalEntry.Base);
        }

        /// <summary>
        ///  A journal file is valid for a journal id if:
        /// - There are no existing transactions in the journal for this journal id
        /// - There *are* existing transactions in the journal *and* that journal is a hard link
        ///
        /// The issue is that we may have a snapshot / manual file move that would result in breaking
        /// of the hard link between shared journals. Consider the case of an index & database that have
        /// a snapshot taken at time T1 for the index and T2 for the database.
        ///
        /// On restore, they journal for the database contains entries for the _index_, but since there is
        /// no hard link after the restore, we miss them (which is fine and expected). But if we link the
        /// current journal file from the data to the index, then on recovery, we'll have transactions in an
        /// out of order manner.
        ///
        /// See: Snapshot_should_have_correct_index_entries_after_snapshot_and_incremental_restore_counters
        /// </summary>
        public bool IsValidFileFor(StorageEnvironment other)
        {
            if (RegisteredEnvironments.ContainsKey(other))
            {
                // already there, so it is fine to not check
                // adding to this is done by EnsureRegistered call that happens 
                // later in the sequence of operations
                return true;
            }

            // if it isn't here, we can safely use it for the other env, because there
            // are no existing transactions to this environment in the file
            if (RecoveredJournalIds.Contains(other.HeaderAccessor.JournalId) is false)
                return true;

            // If this is a newly created file, we don't need to check if there is a link to
            // the file which is the same, important since it saves us two system calls
            // per file per each journal file
            if (NewlyCreatedFile)
                return true;
            
            // there _are_ transactions for the other env in this file, so we now need to 
            // check whatever those are linked or not. If they are _not_ linked, this means that this
            // is a restore of a snapshot or user manually moving files, and writing to this journal
            // may cause a mix of transaction ids, see test:
            // Snapshot_should_have_correct_index_entries_after_snapshot_and_incremental_restore_counters
            return other.Options.IsLinked(other.Journal.CurrentJournalIndex, JournalWriter.FileName.FullPath, out _);
        }
    }
}
