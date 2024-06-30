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
using Voron.Util;
using Constants = Voron.Global.Constants;

namespace Voron.Impl.Journal
{
    public sealed unsafe class JournalFile(StorageEnvironment env, JournalWriter journalWriter, long journalNumber) : IDisposable
    {
        private long _writePosIn4Kb = 0;
        public long LastTransactionId;

        internal List<TransactionHeader> _transactionHeaders = new();

        public override string ToString()
        {
            return string.Format("Number: {0}", Number);
        }

        internal long WritePosIn4KbPosition => Interlocked.Read(ref _writePosIn4Kb);

        public long Number { get; } = journalNumber;


        public long Available4Kbs => journalWriter?.NumberOfAllocated4Kb - _writePosIn4Kb ?? 0;

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

        public void SetLastReadTxHeader(long maxTransactionId, ref TransactionHeader lastReadTxHeader)
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
                    lastReadTxHeader = _transactionHeaders[mid];
                    return;
                }
            }
            if (low == 0)
            {
                lastReadTxHeader.TransactionId = -1; // not found
                return;
            }
            if (high != _transactionHeaders.Count - 1)
            {
                throw new InvalidOperationException("Found a gap in the transaction headers held by this journal file in memory, shouldn't be possible");
            }
            lastReadTxHeader = _transactionHeaders[_transactionHeaders.Count - 1];
        }

        /// <summary>
        /// Write a buffer of transactions (from lazy, usually) to the file
        /// </summary>
        public void Write(long posBy4Kb, byte* p, int numberOf4Kbs)
        {
            int posBy4Kbs = 0;
            while (posBy4Kbs < numberOf4Kbs)
            {
                var readTxHeader = (TransactionHeader*)(p + (posBy4Kbs * 4 * Constants.Size.Kilobyte));
                var totalSize = readTxHeader->CompressedSize != -1 ? readTxHeader->CompressedSize +
                    sizeof(TransactionHeader) : readTxHeader->UncompressedSize + sizeof(TransactionHeader);
                var roundTo4Kb = (totalSize / (4 * Constants.Size.Kilobyte)) +
                                   (totalSize % (4 * Constants.Size.Kilobyte) == 0 ? 0 : 1);
                if (roundTo4Kb > int.MaxValue)
                {
                    MathFailure(numberOf4Kbs);
                }

                // We skip to the next transaction header.
                posBy4Kbs += (int)roundTo4Kb;

                Debug.Assert(readTxHeader->HeaderMarker == Constants.TransactionHeaderMarker);
                _transactionHeaders.Add(*readTxHeader);
            }

            JournalWriter.Write(posBy4Kb, p, numberOf4Kbs);
        }

        private static void MathFailure(int numberOf4Kbs)
        {
            throw new InvalidOperationException("Math failed, total size is larger than 2^31*4KB but we have just: " + numberOf4Kbs + " * 4KB");
        }

        /// <summary>
        /// write transaction's raw page data into journal
        /// </summary>
        public void Write(LowLevelTransaction tx, CompressedPagesResult pages)
        {
            var cur4KbPos = _writePosIn4Kb;

            Debug.Assert(pages.NumberOf4Kbs > 0);

            try
            {
                Write(cur4KbPos, pages.Base, pages.NumberOf4Kbs);
                Interlocked.Add(ref _writePosIn4Kb, pages.NumberOf4Kbs);
                LastTransactionId = tx.Id;
            }
            catch (Exception e)
            {
                env.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                throw;
            }
        }

        public void InitFrom(JournalReader journalReader, List<TransactionHeader> transactionHeaders)
        {
            _writePosIn4Kb = journalReader.Next4Kb;
            _transactionHeaders = [..transactionHeaders];
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
