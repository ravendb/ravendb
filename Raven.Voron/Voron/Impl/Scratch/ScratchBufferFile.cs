// -----------------------------------------------------------------------
//  <copyright file="ScratchBufferFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow;
using System;
using System.Collections.Generic;
using System.Linq;
using Voron.Impl.Paging;
using Voron.Trees;

namespace Voron.Impl.Scratch
{
    public unsafe class ScratchBufferFile : IDisposable
    {
        private class PendingPage
        {
            public long Page;
            public long NumberOfPages;
            public long ValidAfterTransactionId;
        }

        private readonly IVirtualPager _scratchPager;
        private readonly int _scratchNumber;        

        private readonly SortedList<long, long> _freePagesByTransaction = new SortedList<long, long>(NumericDescendingComparer.Instance);
        private readonly Dictionary<long, LinkedList<PendingPage>> _freePagesBySize = new Dictionary<long, LinkedList<PendingPage>>();
        private readonly Dictionary<long, LinkedList<long>> _freePagesBySizeAvailableImmediately = new Dictionary<long, LinkedList<long>>();
        private readonly Dictionary<long, PageFromScratchBuffer> _allocatedPages = new Dictionary<long, PageFromScratchBuffer>();
        
        private long _allocatedPagesCount;
        private long _lastUsedPage;
        private long _txIdAfterWhichLatestFreePagesBecomeAvailable = -1;

        public ScratchBufferFile(IVirtualPager scratchPager, int scratchNumber)
        {
            _scratchPager = scratchPager;
            _scratchNumber = scratchNumber;
            _allocatedPagesCount = 0;
        }

        public PagerState PagerState { get { return _scratchPager.PagerState; } }

        public long TxIdAfterWhichLatestFreePagesBecomeAvailable
        {
            get { return _txIdAfterWhichLatestFreePagesBecomeAvailable; }
        }

        public int Number
        {
            get { return _scratchNumber; }
        }

        public int NumberOfAllocations
        {
            get { return _allocatedPages.Count; }
        }

        public long Size
        {
            get { return _scratchPager.NumberOfAllocatedPages * AbstractPager.PageSize; }
        }

        public long SizeAfterAllocation(long sizeToAllocate)
        {
            return (_lastUsedPage + sizeToAllocate) * AbstractPager.PageSize;
        }

        public PageFromScratchBuffer Allocate(Transaction tx, int numberOfPages, long size)
        {
            _scratchPager.EnsureContinuous(tx, _lastUsedPage, (int)size);

            var result = new PageFromScratchBuffer(_scratchNumber, _lastUsedPage, size, numberOfPages);

            _allocatedPagesCount += numberOfPages;
            _allocatedPages.Add(_lastUsedPage, result);
            _lastUsedPage += size;

            return result;
        }

        public bool HasDiscontinuousSpaceFor(Transaction tx, long size, int scratchFilesInUse)
        {
            long available = 0;
            if (scratchFilesInUse == 1)
            {
                // we can consider the space from the end of a file as available only if we have the single scratch buffer file
                // if a scratch limit is controller over multiple files then we can use only free pages to calculate available space

                available += (_scratchPager.NumberOfAllocatedPages - _lastUsedPage);
            }

            foreach ( var freePage in _freePagesBySizeAvailableImmediately )
                available += freePage.Key * freePage.Value.Count;

            if (available >= size)
                return true;

            var oldestTransaction = tx.Environment.PossibleOldestReadTransaction;

            var sizesFromLargest = _freePagesBySize.Keys.OrderByDescending(x => x);
            foreach (var sizeKey in sizesFromLargest)
            {
                var item = _freePagesBySize[sizeKey].Last;

                while (item != null && item.Value.ValidAfterTransactionId < oldestTransaction)
                {
                    available += sizeKey;

                    if (available >= size)
                        break;

                    item = item.Previous;
                }

                if (available >= size)
                    break;
            }

            return available >= size;
        }

        public bool TryGettingFromAllocatedBuffer(Transaction tx, int numberOfPages, long size, out PageFromScratchBuffer result)
        {
            result = null;

            LinkedList<long> listOfAvailableImmediately;
            if (_freePagesBySizeAvailableImmediately.TryGetValue(size, out listOfAvailableImmediately) && listOfAvailableImmediately.Count > 0)
            {
                var freeAndAvailablePageNumber = listOfAvailableImmediately.Last.Value;

                listOfAvailableImmediately.RemoveLast();

                result = new PageFromScratchBuffer(_scratchNumber, freeAndAvailablePageNumber, size, numberOfPages);

                _allocatedPagesCount += numberOfPages;
                _allocatedPages.Add(freeAndAvailablePageNumber, result);
                return true;
            }

            LinkedList<PendingPage> list;
            if (!_freePagesBySize.TryGetValue(size, out list) || list.Count <= 0)
                return false;

            var val = list.Last.Value;

            if (val.ValidAfterTransactionId >= tx.Environment.PossibleOldestReadTransaction)
                return false;

            list.RemoveLast();
            result = new PageFromScratchBuffer(_scratchNumber, val.Page, size, numberOfPages);

            _allocatedPagesCount += numberOfPages;
            _allocatedPages.Add(val.Page, result);

            return true;       
        }

        public void Free(long page, long asOfTxId)
        {        
            PageFromScratchBuffer value;
            if (_allocatedPages.TryGetValue(page, out value) == false)
            {
                throw new InvalidOperationException("Attempt to free page that wasn't currently allocated: " + page);
            }

            _allocatedPagesCount -= value.NumberOfPages;
            _allocatedPages.Remove(page);

            if (asOfTxId == -1)
            {
                LinkedList<long> list;

                if (_freePagesBySizeAvailableImmediately.TryGetValue(value.Size, out list) == false)
                {
                    list = new LinkedList<long>();
                    _freePagesBySizeAvailableImmediately[value.Size] = list;
                }
                list.AddFirst(value.PositionInScratchBuffer);
            }
            else
            {
                LinkedList<PendingPage> list;

                if (_freePagesBySize.TryGetValue(value.Size, out list) == false)
                {
                    list = new LinkedList<PendingPage>();
                    _freePagesBySize[value.Size] = list;
                }

                list.AddFirst(new PendingPage
                {
                    Page = value.PositionInScratchBuffer,
                    NumberOfPages = value.NumberOfPages,
                    ValidAfterTransactionId = asOfTxId
                });

                // If it is already there we address by position
                int position = _freePagesByTransaction.IndexOfKey(asOfTxId);
                if (position == -1)
                    _freePagesByTransaction.Add(asOfTxId, value.NumberOfPages);
                else
                    _freePagesByTransaction[asOfTxId] = _freePagesByTransaction.Values[position] + value.NumberOfPages;

                if (asOfTxId > _txIdAfterWhichLatestFreePagesBecomeAvailable)
                    _txIdAfterWhichLatestFreePagesBecomeAvailable = asOfTxId;
            }
        }

        public Page ReadPage(Transaction tx, long p, PagerState pagerState = null)
        {
            return _scratchPager.Read(tx, p, pagerState);
        }

        public byte* AcquirePagePointer(Transaction tx, long p)
        {
            return _scratchPager.AcquirePagePointer(tx, p);
        }

        public bool HasActivelyUsedBytes(long oldestActiveTransaction)
        {
            if (_allocatedPagesCount > 0)
                return true;

            if (oldestActiveTransaction > _txIdAfterWhichLatestFreePagesBecomeAvailable)
                return false;

            return true;
        }

        public long ActivelyUsedBytes(long oldestActiveTransaction)
        {
            long result = _allocatedPagesCount;

            var keys = _freePagesByTransaction.Keys;
            var values = _freePagesByTransaction.Values;
            for (var i = keys.Count - 1; i >= 0; i--)
            {
                if (keys[i] < oldestActiveTransaction)
                    break;

                result += values[i];
            }

            return result * AbstractPager.PageSize;
        }

        internal Dictionary<long, long> GetMostAvailableFreePagesBySize()
        {
            return _freePagesBySize.Keys.ToDictionary(size => size, size =>
            {
                var list = _freePagesBySize[size].Last;

                if (list == null)
                    return -1;

                var value = list.Value;
                if (value == null)
                    return -1;

                return value.ValidAfterTransactionId;
            });
        }

        public void Dispose()
        {
            _scratchPager.Dispose();
        }
    }
}
