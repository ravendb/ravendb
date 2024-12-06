// -----------------------------------------------------------------------
//  <copyright file="LogFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.ExceptionServices;
using Sparrow.Logging;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Server;
using Sparrow.Server.Platform;
using Voron.Util;
using Constants = Voron.Global.Constants;
using Voron.Logging;

namespace Voron.Impl.Journal
{
    public sealed unsafe class JournalFile(StorageEnvironment env, JournalWriter journalWriter, long journalNumber) : IDisposable
    {
        public long LastTransactionId;

        internal List<TransactionHeader> _transactionHeaders = new();

        public override string ToString()
        {
            return $"Number: {Number}";
        }

        internal long GetWritePosIn4KbPosition(EnvironmentStateRecord record) => record.Journal.Current == this ? record.Journal.Last4KWritePosition : 0;

        public long Number { get; } = journalNumber;


        public long GetAvailable4Kbs(EnvironmentStateRecord record) => (journalWriter?.NumberOfAllocated4Kb - GetWritePosIn4KbPosition(record)) ?? 0;

        public Size JournalSize => new Size(journalWriter?.NumberOfAllocated4Kb * 4 ?? 0, SizeUnit.Kilobytes);

        internal JournalWriter JournalWriter => journalWriter;

        public void Release()
        {
            if (journalWriter?.Release() != true)
                return;

            Dispose();
        }

        public void AddRef()
        {
            journalWriter?.AddRef();
        }

        public void Dispose()
        {
            _transactionHeaders = null;
            journalWriter = null;
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
        public long Write(long posBy4Kb, Span<Pal.jounral_entry> entries)
        {
            long totalNumberOf4Kbs = 0;
            for (int i = 0; i < entries.Length; i++)
            {
                var readTxHeader = (TransactionHeader*)entries[i].Base;
                totalNumberOf4Kbs += entries[i].NumberOf4Kbs;
                Debug.Assert(readTxHeader->HeaderMarker == Constants.TransactionHeaderMarker);
                _transactionHeaders.Add(*readTxHeader);
            }

            fixed (Pal.jounral_entry* pEntries = entries)
            {
                JournalWriter.Write(posBy4Kb, pEntries, entries.Length, totalNumberOf4Kbs);
            }
            
            return totalNumberOf4Kbs;
        }

        /// <summary>
        /// write transaction's raw page data into journal
        /// </summary>
        public void Write(LowLevelTransaction tx, Span<Pal.jounral_entry> pages)
        {
            var cur4KbPos = tx.CurrentStateRecord.Journal.Current == this ? tx.CurrentStateRecord.Journal.Last4KWritePosition : 0;

            Debug.Assert(pages.IsEmpty is false && pages[0].NumberOf4Kbs > 0, "pages.IsEmpty is false && pages[0].NumberOf4Kbs > 0");

            try
            {
                long totalSizeIn4Kbs = Write(cur4KbPos, pages);
                tx.UpdateJournal(this, cur4KbPos + totalSizeIn4Kbs);
                LastTransactionId = tx.Id;
            }
            catch (Exception e)
            {
                env.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                throw;
            }
        }

        public void InitFrom(StorageEnvironment storageEnvironment, JournalReader journalReader, List<TransactionHeader> transactionHeaders)
        {
            storageEnvironment.UpdateJournal(this, journalReader.Next4Kb);
            _transactionHeaders = [.. transactionHeaders];
        }

        public bool DeleteOnClose
        {
            set
            {
                var writer = journalWriter;

                if (writer != null)
                    writer.DeleteOnClose = value;
            }
        }
    }
}
