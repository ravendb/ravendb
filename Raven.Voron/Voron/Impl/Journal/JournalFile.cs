// -----------------------------------------------------------------------
//  <copyright file="LogFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Voron.Impl.Paging;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public unsafe class JournalFile : IDisposable
    {
        private readonly IJournalWriter _journalWriter;
        private long _writePage;
        private bool _disposed;
        private int _refs;
        private readonly PageTable _pageTranslationTable = new PageTable();
        private readonly List<PagePosition> _unusedPages = new List<PagePosition>();
        private readonly object _locker = new object();

        public JournalFile(IJournalWriter journalWriter, long journalNumber)
        {
            Number = journalNumber;
            _journalWriter = journalWriter;
            _writePage = 0;
        }

        public override string ToString()
        {
            return string.Format("Number: {0}", Number);
        }

        public JournalFile(IJournalWriter journalWriter, long journalNumber, long lastSyncedPage)
            : this(journalWriter, journalNumber)
        {
            _writePage = lastSyncedPage + 1;
        }

#if DEBUG
        private readonly StackTrace _st = new StackTrace(true);
#endif

        ~JournalFile()
        {
            Dispose();

#if DEBUG
            Trace.WriteLine(
                "Disposing a journal file from finalizer! It should be disposed by using JournalFile.Release() instead!. Log file number: " +
                Number + ". Number of references: " + _refs + " " + _st);
#endif
        }

        internal long WritePagePosition
        {
            get { return _writePage; }
        }

        public long Number { get; private set; }


        public long AvailablePages
        {
            get { return _journalWriter.NumberOfAllocatedPages - _writePage; }
        }

        internal IJournalWriter JournalWriter
        {
            get { return _journalWriter; }
        }

        public PageTable PageTranslationTable
        {
            get { return _pageTranslationTable; }
        }

        public void Release()
        {
            if (Interlocked.Decrement(ref _refs) != 0)
                return;

            Dispose();
        }

        public void AddRef()
        {
            Interlocked.Increment(ref _refs);
        }

        public void Dispose()
        {
            DisposeWithoutClosingPager();
            _journalWriter?.Dispose();
        }

        public JournalSnapshot GetSnapshot()
        {
            var lastTxId = _pageTranslationTable.GetLastSeenTransactionId();
            return new JournalSnapshot
            {
                Number = Number,
                AvailablePages = AvailablePages,
                PageTranslationTable = _pageTranslationTable,
                LastTransaction = lastTxId
            };
        }

        public void DisposeWithoutClosingPager()
        {
            if (_disposed)
                return;

            GC.SuppressFinalize(this);

            _disposed = true;
        }

        public bool ReadTransaction(long pos, TransactionHeader* txHeader)
        {
            return _journalWriter.Read(pos, (byte*)txHeader, sizeof(TransactionHeader));
        }

        /// <summary>
        /// write transaction's raw page data into journal. returns write page position
        /// </summary>
        public long Write(Transaction tx, IntPtr[] pages)
        {
            var ptt = new Dictionary<long, PagePosition>(NumericEqualityComparer.Instance);
            var unused = new HashSet<PagePosition>();
            var pageWritePos = _writePage;

            UpdatePageTranslationTable(tx, unused, ptt);

            lock (_locker)
            {
                _writePage += pages.Length;
                _pageTranslationTable.SetItems(tx, ptt);

                Debug.Assert(!_unusedPages.Any(unused.Contains));
                _unusedPages.AddRange(unused);
            }

            var position = pageWritePos * AbstractPager.PageSize;
            _journalWriter.WriteGather(position, pages);

            return pageWritePos;
        }      

        private void UpdatePageTranslationTable(Transaction tx, HashSet<PagePosition> unused, Dictionary<long, PagePosition> ptt)
        {
            foreach (var freedPageNumber in tx.GetFreedPagesNumbers())
            {
                // set freed page marker - note it can be overwritten below by later allocation

                ptt[freedPageNumber] = new PagePosition
                {
                    ScratchPos = -1,
                    ScratchNumber = -1,
                    TransactionId = tx.Id,
                    JournalNumber = Number,
                    IsFreedPageMarker = true
                };
            }

            var txPages = tx.GetTransactionPages();
            foreach ( var txPage in txPages )
            {
                var scratchPage = tx.Environment.ScratchBufferPool.ReadPage(tx, txPage.ScratchFileNumber, txPage.PositionInScratchBuffer);
                var pageNumber = scratchPage.PageNumber;

                PagePosition value;
                if (_pageTranslationTable.TryGetValue(tx, pageNumber, out value))
                {
                    value.UnusedInPTT = true;
                    unused.Add(value);
                }

                PagePosition pagePosition;
                if (ptt.TryGetValue(pageNumber, out pagePosition) && pagePosition.IsFreedPageMarker == false)
                    unused.Add(pagePosition);

                ptt[pageNumber] = new PagePosition
                {
                    ScratchPos = txPage.PositionInScratchBuffer,
                    ScratchNumber = txPage.ScratchFileNumber,
                    TransactionId = tx.Id,
                    JournalNumber = Number
                };
            }

            foreach (var freedPage in tx.GetUnusedScratchPages())
            {
                unused.Add(new PagePosition
                {
                    ScratchPos = freedPage.PositionInScratchBuffer,
                    ScratchNumber = freedPage.ScratchFileNumber,
                    TransactionId = tx.Id,
                    JournalNumber = Number
                });
            }
        }

        public void InitFrom(JournalReader journalReader)
        {
            _writePage = journalReader.NextWritePage;
        }

        public bool DeleteOnClose { set { _journalWriter.DeleteOnClose = value; } }
        
        public void FreeScratchPagesOlderThan(Transaction tx, long lastSyncedTransactionId, bool forceToFreeAllPages = false)
        {
            if (tx == null) throw new ArgumentNullException("tx");
            var unusedPages = new List<PagePosition>();

            List<PagePosition> unusedAndFree;
            List<long> keysToRemove;

            lock (_locker)
            {
                unusedAndFree = _unusedPages.FindAll(position => position.TransactionId <= lastSyncedTransactionId);
                _unusedPages.RemoveAll(position => position.TransactionId <= lastSyncedTransactionId);

                if (forceToFreeAllPages)
                    keysToRemove = _pageTranslationTable.KeysWhereSomePagesOlderThan(lastSyncedTransactionId);
                else
                    keysToRemove = _pageTranslationTable.KeysWhereAllPagesOlderThan(lastSyncedTransactionId);

                _pageTranslationTable.Remove(keysToRemove, lastSyncedTransactionId, unusedPages);
            }

            foreach (var unusedScratchPage in unusedAndFree)
            {
                if (unusedScratchPage.IsFreedPageMarker)
                    continue;

                tx.Environment.ScratchBufferPool.Free(unusedScratchPage.ScratchNumber, unusedScratchPage.ScratchPos, tx.Id);
            }

            foreach (var page in unusedPages)
            {
                if (page.IsFreedPageMarker)
                    continue;

                if (page.UnusedInPTT) // to prevent freeing a page that was already freed as unusedAndFree
                {
                    // the page could be either freed in the current run, then just skip it to avoid freeing an unallocated page, or
                    // it could be released in an earlier run, but it still resided in PTT because a under a relevant page number of PTT 
                    // there were overwrites by newer transactions (> lastSyncedTransactionId) and we didn't remove it from there
                    continue;
                }

                tx.Environment.ScratchBufferPool.Free(page.ScratchNumber, page.ScratchPos, tx.Id);
            }
        }
    }
}
