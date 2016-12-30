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
using System.Threading;
using Voron.Global;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public unsafe class JournalFile : IDisposable
    {
        private readonly StorageEnvironment _env;
        private IJournalWriter _journalWriter;
        private long _writePosIn4Kb;
        private bool _disposed;
        private int _refs;
        private readonly PageTable _pageTranslationTable = new PageTable();

        private readonly HashSet<PagePosition> _unusedPagesHashSetPool = new HashSet<PagePosition>(PagePositionEqualityComparer.Instance);

        private readonly List<PagePosition> _unusedPages;
        private readonly object _locker = new object();

        public JournalFile(StorageEnvironment env, IJournalWriter journalWriter, long journalNumber)
        {
            Number = journalNumber;
            _env = env;
            _journalWriter = journalWriter;
            _writePosIn4Kb = 0;
            _unusedPages = new List<PagePosition>();

        }

        public override string ToString()
        {
            return string.Format("Number: {0}", Number);
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

        internal long WritePosIn4KbPosition => _writePosIn4Kb;

        public long Number { get; }


        public long Available4Kbs => _journalWriter?.NumberOfAllocated4Kb - _writePosIn4Kb ?? 0;

        internal IJournalWriter JournalWriter => _journalWriter;

        public PageTable PageTranslationTable => _pageTranslationTable;

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
                FileInstance = this,
                Number = Number,
                Available4Kbs = Available4Kbs,
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
            var pageWritePos = _writePosIn4Kb;


            _unusedPagesHashSetPool.Clear();
            UpdatePageTranslationTable(tx, _unusedPagesHashSetPool, ptt);

            lock (_locker)
            {
                _writePosIn4Kb += pages.NumberOf4Kbs;

                Debug.Assert(!_unusedPages.Any(_unusedPagesHashSetPool.Contains));
                _unusedPages.AddRange(_unusedPagesHashSetPool);
            }
            _unusedPagesHashSetPool.Clear();

            if (tx.IsLazyTransaction == false && (lazyTransactionScratch == null || lazyTransactionScratch.HasDataInBuffer() == false))
            {
                try
                {
                    _journalWriter.Write(pageWritePos, pages.Base, pages.NumberOf4Kbs);
                }
                catch (Exception e)
                {
                    _env.CatastrophicFailure = ExceptionDispatchInfo.Capture(e);
                    throw;
                }
            }
            else
            {
                if (lazyTransactionScratch == null)
                    throw new InvalidOperationException("lazyTransactionScratch cannot be null if the transaction is lazy (or a previous one was)");
                lazyTransactionScratch.EnsureSize(_journalWriter.NumberOfAllocated4Kb);
                lazyTransactionScratch.AddToBuffer(pageWritePos, pages, uncompressedPageCount);

                // non lazy tx will add itself to the buffer and then flush scratch to journal
                if (tx.IsLazyTransaction == false ||
                    lazyTransactionScratch.NumberOfPages > tx.Environment.ScratchBufferPool.GetAvailablePagesCount()/2)
                {
                    try
                    {
                        lazyTransactionScratch.WriteBufferToFile(this, tx);
                    }
                    catch (Exception e)
                    {
                        _env.CatastrophicFailure = ExceptionDispatchInfo.Capture(e);
                        throw;
                    }
                }
                else 
                {
                    lazyTransactionScratch.EnsureHasExistingReadTransaction(tx);
                }
            }

            lock (_locker)
            {
                _pageTranslationTable.SetItems(tx, ptt);
            }

        }

        private void UpdatePageTranslationTable(LowLevelTransaction tx, HashSet<PagePosition> unused, Dictionary<long, PagePosition> ptt)
        {
            long journalNumber = Number;

            foreach (var freedPageNumber in tx.GetFreedPagesNumbers())
            {
                // set freed page marker - note it can be overwritten below by later allocation

                ptt[freedPageNumber] = new PagePosition(-1, tx.Id, journalNumber, -1, true);
            }

            var scratchBufferPool = tx.Environment.ScratchBufferPool;
            var txPages = tx.GetTransactionPages();
            foreach (var txPage in txPages)
            {
                var scratchPage = scratchBufferPool.ReadPage(tx, txPage.ScratchFileNumber, txPage.PositionInScratchBuffer);
                var pageNumber = scratchPage.PageNumber;

                PagePosition value;
                if (_pageTranslationTable.TryGetValue(tx, pageNumber, out value))
                {
                    value.UnusedInPTT = true;
                    unused.Add(value);
                }

                PagePosition pagePosition;
                if (ptt.TryGetValue(pageNumber, out pagePosition) && pagePosition.IsFreedPageMarker == false)
                {
                    unused.Add(pagePosition);
                }                                                    
                                
                ptt[pageNumber] = new PagePosition(txPage.PositionInScratchBuffer, tx.Id, journalNumber, txPage.ScratchFileNumber);
            }

            foreach (var freedPage in tx.GetUnusedScratchPages())
            {
                unused.Add(new PagePosition(freedPage.PositionInScratchBuffer, tx.Id, journalNumber, freedPage.ScratchFileNumber));
            }
        }

        public void InitFrom(JournalReader journalReader)
        {
            _writePosIn4Kb = journalReader.NextWritePage;
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

              _pageTranslationTable.RemoveKeysWhereAllPagesOlderThan(lastSyncedTransactionId, unusedPages);
            }

            foreach (var unusedScratchPage in unusedAndFree)
            {
                if (unusedScratchPage.IsFreedPageMarker)
                    continue;

                tx.Environment.ScratchBufferPool.Free(unusedScratchPage.ScratchNumber, unusedScratchPage.ScratchPos, tx);
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

                tx.Environment.ScratchBufferPool.Free(page.ScratchNumber, page.ScratchPos, tx);
            }
        }
    }
}
