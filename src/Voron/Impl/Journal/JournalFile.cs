// -----------------------------------------------------------------------
//  <copyright file="LogFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow;
using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
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
        
        public override string ToString()
        {
            return $"Number: {Number}";
        }

        internal long GetWritePosIn4KbPosition(EnvironmentStateRecord record) => record.Journal.Number == Number ? record.Journal.Last4KWritePosition : 0;

        public long Number { get; } = journalNumber;

        public SingleUseFlag DoneWriting;
        private JournalWriter _journalWriter = journalWriter;

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

        public TransactionHeader GetLastReadTxHeader(long maxTransactionId)
        {
            int low = 0;
            int high = _transactionHeaders.Count - 1;

            while (low <= high)
            {
                int mid = (low + high) >> 1;
                long midValTxId = _transactionHeaders[mid].TransactionId;

                if (midValTxId < maxTransactionId)
                    low = mid + 1;
                else if (midValTxId > maxTransactionId)
                    high = mid - 1;
                else // found the max tx id
                {
                    return _transactionHeaders[mid];
                }
            }
            if (low == 0)
            {
                return new TransactionHeader{ TransactionId = -1}; // not found
            }
            if (high != _transactionHeaders.Count - 1)
            {
                throw new InvalidOperationException("Found a gap in the transaction headers held by this journal file in memory, shouldn't be possible");
            }
            return _transactionHeaders[^1];
        }

        /// <summary>
        /// Write a buffer of transactions (from lazy, usually) to the file
        /// </summary>
        public long Write(long posBy4Kb, Span<Pal.journal_entry> entries)
        {
            long totalNumberOf4Kbs = 0;
            for (int i = 0; i < entries.Length; i++)
            {
                var readTxHeader = (TransactionHeader*)entries[i].Base;
                totalNumberOf4Kbs += entries[i].NumberOf4Kbs;
                Debug.Assert(readTxHeader->HeaderMarker == Constants.TransactionHeaderMarker);
            }

            JournalWriter.Write(posBy4Kb, entries, totalNumberOf4Kbs);
            
            return totalNumberOf4Kbs;
        }

        /// <summary>
        /// write transaction's raw page data into journal
        /// </summary>
        public long Write(LowLevelTransaction tx, Span<Pal.journal_entry> pages)
        {
            var cur4KbPos = tx.CurrentStateRecord.Journal.Number == Number ? tx.CurrentStateRecord.Journal.Last4KWritePosition : 0;

            Debug.Assert(pages.IsEmpty is false && pages[0].NumberOf4Kbs > 0, "pages.IsEmpty is false && pages[0].NumberOf4Kbs > 0");

            try
            {
                long totalSizeIn4Kbs = Write(cur4KbPos, pages);
                LastTransactionId = tx.Id;
                return cur4KbPos + totalSizeIn4Kbs;
            }
            catch (Exception e)
            {
                env.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                throw;
            }
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
            
            // there _are_ transactions for the other env in this file, so we now need to 
            // check whatever those are linked or not. If they are _not_ linked, this means that this
            // is a restore of a snapshot or user manually moving files, and writing to this journal
            // may cause a mix of transaction ids, see test:
            // Snapshot_should_have_correct_index_entries_after_snapshot_and_incremental_restore_counters
            return other.Options.IsLinked(other.Journal.CurrentJournalIndex, JournalWriter.FileName.FullPath, out _);
        }
    }
}
