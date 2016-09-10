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
using System.Threading;

using Voron.Util;

namespace Voron.Impl.Journal
{
    public unsafe class JournalFile : IDisposable
    {
        private IJournalWriter _journalWriter;
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


        ~JournalFile()
        {
            Dispose();

#if DEBUG
            Debug.WriteLine(
                "Disposing a journal file from finalizer! It should be disposed by using JournalFile.Release() instead!. Log file number: " +
                Number + ". Number of references: " + _refs);
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
            _journalWriter = null;
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
        /// write transaction's raw page data into journal
        /// </summary>
        public void Write(LowLevelTransaction tx, CompressedPagesResult pages, LazyTransactionBuffer lazyTransactionScratch, int uncompressedPageCount)
        {
            var ptt = new Dictionary<long, PagePosition>(NumericEqualityComparer.Instance);
            var unused = new HashSet<PagePosition>();
            var pageWritePos = _writePage;

            UpdatePageTranslationTable(tx, unused, ptt);

            lock (_locker)
            {
                _writePage += pages.NumberOfPages;
                _pageTranslationTable.SetItems(tx, ptt);

                Debug.Assert(!_unusedPages.Any(unused.Contains));
                _unusedPages.AddRange(unused);
            }

            var position = pageWritePos * tx.Environment.Options.PageSize;

            if (tx.IsLazyTransaction == false && (lazyTransactionScratch == null || lazyTransactionScratch.HasDataInBuffer() == false))
            {
                _journalWriter.WritePages(position, pages.Base, pages.NumberOfPages);
            }
            else
            {
                if (lazyTransactionScratch == null)
                    throw new InvalidOperationException("lazyTransactionScratch cannot be null if the transaction is lazy (or a previous one was)");
                lazyTransactionScratch.EnsureSize(_journalWriter.NumberOfAllocatedPages);
                lazyTransactionScratch.AddToBuffer(position, pages, uncompressedPageCount);

                // non lazy tx will add itself to the buffer and then flush scratch to journal
                if (tx.IsLazyTransaction == false ||
                    lazyTransactionScratch.NumberOfPages > tx.Environment.ScratchBufferPool.GetAvailablePagesCount()/2)
                {
                    lazyTransactionScratch.WriteBufferToFile(this, tx);
                }
                else 
                {
                    lazyTransactionScratch.EnsureHasExistingReadTransaction(tx);
                }
            }
        }

        private void UpdatePageTranslationTable(LowLevelTransaction tx, HashSet<PagePosition> unused, Dictionary<long, PagePosition> ptt)
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
            foreach (var txPage in txPages)
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

        public void FreeScratchPagesOlderThan(LowLevelTransaction tx, long lastSyncedTransactionId)
        {
            if (tx == null) throw new ArgumentNullException(nameof(tx));
            var unusedPages = new List<PagePosition>();

            List<PagePosition> unusedAndFree;

            lock (_locker)
            {
                unusedAndFree = _unusedPages.FindAll(position => position.TransactionId <= lastSyncedTransactionId);
                _unusedPages.RemoveAll(position => position.TransactionId <= lastSyncedTransactionId);

                var keysToRemove = 
                    _pageTranslationTable.KeysWhereAllPagesOlderThan(lastSyncedTransactionId);

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
