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
using Voron.Global;
using Voron.Util;
using System.Buffers;

namespace Voron.Impl.Journal
{
    public unsafe class JournalFile : IDisposable
    {
        private readonly StorageEnvironment _env;
        private IJournalWriter _journalWriter;
        private long _writePosIn4Kb;

        private TransactionHeader[] _transactionHeaders;
        private int _numberOfTransactionHeaders;

        private readonly PageTable _pageTranslationTable = new PageTable();

        private readonly HashSet<PagePosition> _unusedPagesHashSetPool = new HashSet<PagePosition>();

        private readonly FastList<PagePosition> _unusedPages;
        private readonly ContentionLoggingLocker _locker2;

        public JournalFile(StorageEnvironment env, IJournalWriter journalWriter, long journalNumber)
        {
            Number = journalNumber;
            _env = env;
            _transactionHeaders = ArrayPool<TransactionHeader>.Shared.Rent(journalWriter.NumberOfAllocated4Kb);
            _journalWriter = journalWriter;
            _writePosIn4Kb = 0;
            _unusedPages = new FastList<PagePosition>();
            var logger = LoggingSource.Instance.GetLogger<JournalFile>(JournalWriter.FileName.FullPath);
            _locker2 = new ContentionLoggingLocker(logger, JournalWriter.FileName.FullPath);
        }

        public override string ToString()
        {
            return string.Format("Number: {0}", Number);
        }


        internal long WritePosIn4KbPosition => Interlocked.Read(ref _writePosIn4Kb);

        public long Number { get; }


        public long Available4Kbs => _journalWriter?.NumberOfAllocated4Kb - _writePosIn4Kb ?? 0;

        internal IJournalWriter JournalWriter => _journalWriter;

        public PageTable PageTranslationTable => _pageTranslationTable;

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
            if (_transactionHeaders != null)
                ArrayPool<TransactionHeader>.Shared.Return(_transactionHeaders);
            _transactionHeaders = null;
            _unusedPagesHashSetPool.Clear();
            _unusedPages.Clear();
            _pageTranslationTable.Clear();
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
                WritePosIn4KbPosition = WritePosIn4KbPosition,
                PageTranslationTable = _pageTranslationTable,
                LastTransaction = lastTxId
            };
        }

        public void SetLastReadTxHeader(long maxTransactionId, ref TransactionHeader lastReadTxHeader)
        {
            int low = 0;
            int high = _numberOfTransactionHeaders-1;

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
            if(low == 0)
            {
                lastReadTxHeader.TransactionId = -1; // not found
                return;
            }
            if(high != _numberOfTransactionHeaders - 1)
            {
                throw new InvalidOperationException("Found a gap in the transaction headers held by this journal file in memory, shouldn't be possible");
            }
            lastReadTxHeader = _transactionHeaders[_numberOfTransactionHeaders - 1];
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
                posBy4Kbs += (int)roundTo4Kb ;
                if(_transactionHeaders.Length == _numberOfTransactionHeaders)
                {
                    var temp = ArrayPool<TransactionHeader>.Shared.Rent(_transactionHeaders.Length * 2);
                    Array.Copy(_transactionHeaders, temp, _transactionHeaders.Length);
                    ArrayPool<TransactionHeader>.Shared.Return(_transactionHeaders);
                    _transactionHeaders = temp;
                }
                Debug.Assert(readTxHeader->HeaderMarker == Constants.TransactionHeaderMarker);
                _transactionHeaders[_numberOfTransactionHeaders++] = *readTxHeader;
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
        public UpdatePageTranslationTableAndUnusedPagesAction Write(LowLevelTransaction tx, CompressedPagesResult pages, LazyTransactionBuffer lazyTransactionScratch)
        {
            var ptt = new Dictionary<long, PagePosition>(NumericEqualityComparer.BoxedInstanceInt64);
            var cur4KbPos = _writePosIn4Kb;

            Debug.Assert(pages.NumberOf4Kbs > 0);

            UpdatePageTranslationTable(tx, _unusedPagesHashSetPool, ptt);
            
            if (tx.IsLazyTransaction == false && (lazyTransactionScratch == null || lazyTransactionScratch.HasDataInBuffer() == false))
            {
                try
                {
                    Write(cur4KbPos, pages.Base, pages.NumberOf4Kbs);
                }
                catch (Exception e)
                {
                    _env.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                    throw;
                }
            }
            else
            {
                if (lazyTransactionScratch == null)
                    throw new InvalidOperationException("lazyTransactionScratch cannot be null if the transaction is lazy (or a previous one was)");

                var sizeInBytes = _journalWriter.NumberOfAllocated4Kb * 4 * Constants.Size.Kilobyte;

                int sizeInPages = checked(sizeInBytes / Constants.Storage.PageSize +
                                          sizeInBytes % Constants.Storage.PageSize == 0 ? 0 : 1);

                lazyTransactionScratch.EnsureSize(sizeInPages);
                lazyTransactionScratch.AddToBuffer(cur4KbPos, pages);

                // non lazy tx will add itself to the buffer and then flush scratch to journal
                if (tx.IsLazyTransaction == false ||
                    lazyTransactionScratch.NumberOfPages > tx.Environment.ScratchBufferPool.GetAvailablePagesCount() / 2)
                {
                    try
                    {
                        lazyTransactionScratch.WriteBufferToFile(this, tx);
                    }
                    catch (Exception e)
                    {
                        _env.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                        throw;
                    }
                }
                else
                {
                    lazyTransactionScratch.EnsureHasExistingReadTransaction(tx);
                }
            }

            return new UpdatePageTranslationTableAndUnusedPagesAction(this, tx, ptt, pages.NumberOf4Kbs);
        }

        private void UpdatePageTranslationTable(LowLevelTransaction tx, HashSet<PagePosition> unused, Dictionary<long, PagePosition> ptt)
        {
            // REVIEW: This number do not grow easily. There is no way we can go higher than int.MaxValue
            //         Make sure that we upgrade later upwards so journal numbers are always ints.
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
                long pageNumber = txPage.ScratchPageNumber;
                if (pageNumber == -1) // if we don't already have it from TX preparing then ReadPage
                {
                var scratchPage = scratchBufferPool.ReadPage(tx, txPage.ScratchFileNumber, txPage.PositionInScratchBuffer);
                    pageNumber = scratchPage.PageNumber;
                }

                Debug.Assert(pageNumber >= 0);
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

        public void InitFrom(JournalReader journalReader, TransactionHeader[] transactionHeaders, int transactionsCount)
        {
            _writePosIn4Kb = journalReader.Next4Kb;
            Array.Copy(transactionHeaders, _transactionHeaders, transactionsCount);
        }

        public bool DeleteOnClose { set { _journalWriter.DeleteOnClose = value; } }
        
        
        private static readonly ObjectPool<FastList<PagePosition>, FastList<PagePosition>.ResetBehavior> _scratchPagesPositionsPool = new ObjectPool<FastList<PagePosition>, FastList<PagePosition>.ResetBehavior>(() => new FastList<PagePosition>(), 10);

        public void FreeScratchPagesOlderThan(LowLevelTransaction tx, long lastSyncedTransactionId)
        {            
            var unusedPages = _scratchPagesPositionsPool.Allocate();
            var unusedAndFree = _scratchPagesPositionsPool.Allocate();

            using (_locker2.Lock())
            {                
                int count = _unusedPages.Count;
                int originalCount = count;
                
                for (int i = 0; i < count; i++)
                {
                    var page = _unusedPages[i];
                    if (page.TransactionId <= lastSyncedTransactionId)
                    {
                        unusedAndFree.Add(page);

                        count--;                        
                        
                        if ( i < count )
                            _unusedPages[i] = _unusedPages[count];
                        
                        _unusedPages.RemoveAt(count);

                        i--;
                    }
                }

                // This must hold true, if not we have leakage of memory and disk.
                Debug.Assert(_unusedPages.Count + unusedAndFree.Count == originalCount);

                _pageTranslationTable.RemoveKeysWhereAllPagesOlderThan(lastSyncedTransactionId, unusedPages);
            }

            // use current write tx id to prevent from overriding a scratch page by write tx 
            // while there might be old read tx looking at it by using PTT from the journal snapshot
            var availableForAllocationAfterTx = tx.Id;

            int length = unusedPages.Count;
            for (int i = 0; i < length; i++)
            {
                var page = unusedPages[i];

                if (page.IsFreedPageMarker)
                    continue;

                if (page.UnusedInPTT) // to prevent freeing a page that was already freed as unused and free
                {
                    // the page could be either freed in the current run, then just skip it to avoid freeing an unallocated page, or
                    // it could be released in an earlier run, but it still resided in PTT because a under a relevant page number of PTT 
                    // there were overwrites by newer transactions (> lastSyncedTransactionId) and we didn't remove it from there
                    continue;
                }

                unusedAndFree.Add(page);
            }

            length = unusedAndFree.Count;
            for (int i = 0; i < length; i++)
            {
                var unusedPage = unusedAndFree[i];
                if (unusedPage.IsFreedPageMarker)
                    continue;

                _env.ScratchBufferPool.Free(tx, unusedPage.ScratchNumber, unusedPage.ScratchPage, availableForAllocationAfterTx);
            }

            _scratchPagesPositionsPool.Free(unusedPages);
            _scratchPagesPositionsPool.Free(unusedAndFree);
        }

        public struct UpdatePageTranslationTableAndUnusedPagesAction
        {
            private readonly JournalFile _parent;
            private readonly LowLevelTransaction _tx;
            private readonly Dictionary<long, PagePosition> _ptt;
            private readonly int _numberOfWritten4Kbs;

            public UpdatePageTranslationTableAndUnusedPagesAction(JournalFile parent, LowLevelTransaction tx, Dictionary<long, PagePosition> ptt, int numberOfWritten4Kbs)
            {
                _parent = parent;
                _tx = tx;
                _ptt = ptt;
                _numberOfWritten4Kbs = numberOfWritten4Kbs;
            }

            public void ExecuteAfterCommit()
            {
                Debug.Assert(_tx.Committed);

                using (_parent._locker2.Lock())
                {
                    Debug.Assert(!_parent._unusedPages.Any(_parent._unusedPagesHashSetPool.Contains)); // We ensure there cannot be duplicates here (disjoint sets). 

                    foreach (var item in _parent._unusedPagesHashSetPool)
                        _parent._unusedPages.Add(item);

                    _parent._pageTranslationTable.SetItems(_tx, _ptt);
                    // it is important that the last write position will be set
                    // _after_ the PTT update, because a flush that is concurrent 
                    // with the write will first get the WritePosIn4KB and then 
                    // do the flush based on the PTT. Worst case, we'll flush 
                    // more then we need, but won't get into a position where we
                    // think we flushed, and then realize that we didn't.
                    Interlocked.Add(ref _parent._writePosIn4Kb, _numberOfWritten4Kbs);
                }

                _parent._unusedPagesHashSetPool.Clear();
            }
        }
    }
}
