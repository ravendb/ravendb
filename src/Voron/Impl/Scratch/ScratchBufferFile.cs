// -----------------------------------------------------------------------
//  <copyright file="ScratchBufferFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow;
using Sparrow.Collections;
using System;
using System.Collections.Generic;
using System.Linq;
using Voron.Impl.Paging;
using Voron.Util;

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
        

        private long _allocatedPagesUsedSize;
        private long _lastUsedPage;

        public ScratchBufferFile(IVirtualPager scratchPager, int scratchNumber)
        {
            _scratchPager = scratchPager;
            _scratchNumber = scratchNumber;
            _allocatedPagesUsedSize = 0;
        }

        public PagerState PagerState => _scratchPager.PagerState;

        public int Number => _scratchNumber;

        public int NumberOfAllocations => _allocatedPages.Count;

        public long Size => _scratchPager.NumberOfAllocatedPages * _scratchPager.PageSize;

        public long SizeAfterAllocation(long sizeToAllocate)
        {
            return (_lastUsedPage + sizeToAllocate) * _scratchPager.PageSize;
        }

        public PageFromScratchBuffer Allocate(LowLevelTransaction tx, int numberOfPages, int sizeToAllocate)
        {
            var pagerState = _scratchPager.EnsureContinuous(_lastUsedPage, sizeToAllocate);
            tx.EnsurePagerStateReference(pagerState);

            var result = new PageFromScratchBuffer(_scratchNumber, _lastUsedPage, sizeToAllocate, numberOfPages);

            _allocatedPagesUsedSize += numberOfPages;
            _allocatedPages.Add(_lastUsedPage, result);
            _lastUsedPage += sizeToAllocate;

            return result;
        }

        public bool HasDiscontinuousSpaceFor(LowLevelTransaction tx, long size, int scratchFilesInUse)
        {
            long available = 0;

            if (scratchFilesInUse == 1)
            {
                // we can consider the space from the end of a file as available only if we have the single scratch buffer file
                // if a scratch limit is controller over multiple files then we can use only free pages to calculate available space

                available += (_scratchPager.NumberOfAllocatedPages - _lastUsedPage);
            }

            foreach (var freePage in _freePagesBySizeAvailableImmediately)
                available += freePage.Key * freePage.Value.Count;


            if (available >= size)
                return true;


            var oldestTransaction = tx.Environment.OldestTransaction;

            var sizesFromLargest = _freePagesBySize.Keys.OrderByDescending(x => x);

            foreach (var sizeKey in sizesFromLargest)
            {
                var item = _freePagesBySize[sizeKey].Last;

                while (item != null && (oldestTransaction == 0 || item.Value.ValidAfterTransactionId < oldestTransaction))
                {
                    available += sizeKey;

                    if(available >= size)
                        break;

                    item = item.Previous;
                }

                if(available >= size)
                    break;
            }

            return available >= size;
        }

        public bool TryGettingFromAllocatedBuffer(LowLevelTransaction tx, int numberOfPages, long size, out PageFromScratchBuffer result)
        {
            result = null;

            LinkedList<long> listOfAvailableImmediately;
            if (_freePagesBySizeAvailableImmediately.TryGetValue(size, out listOfAvailableImmediately) && listOfAvailableImmediately.Count > 0)
            {
                var freeAndAvailablePageNumber = listOfAvailableImmediately.Last.Value;

                listOfAvailableImmediately.RemoveLast();

                result = new PageFromScratchBuffer ( _scratchNumber, freeAndAvailablePageNumber, size, numberOfPages );
                
                _allocatedPagesUsedSize += numberOfPages;
                _allocatedPages.Add(freeAndAvailablePageNumber, result);
                return true;
            }

            LinkedList<PendingPage> list;
            if (!_freePagesBySize.TryGetValue(size, out list) || list.Count <= 0)
                return false;

            var val = list.Last.Value;
            var oldestTransaction = tx.Environment.OldestTransaction;
            if (oldestTransaction != 0 && val.ValidAfterTransactionId >= oldestTransaction) // OldestTransaction can be 0 when there are none other transactions and we are in process of new transaction header allocation
                return false;

            list.RemoveLast();
            result = new PageFromScratchBuffer ( _scratchNumber, val.Page, size, numberOfPages );

            _allocatedPagesUsedSize += numberOfPages;
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

            _allocatedPagesUsedSize -= value.NumberOfPages;
            _allocatedPages.Remove(page);

            if (value.Size == 0)
                return;// this value was broken up to smaller sections, only the first page there is valid for space allocations
            
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
                int position =  _freePagesByTransaction.IndexOfKey(asOfTxId);
                if (position == -1)
                    _freePagesByTransaction.Add(asOfTxId, value.NumberOfPages);
                else
                    _freePagesByTransaction[asOfTxId] = _freePagesByTransaction.Values[position] + value.NumberOfPages;
            }
        }

        public Page ReadPage(LowLevelTransaction tx, long p, PagerState pagerState = null)
        {
            return new Page(_scratchPager.AcquirePagePointer(tx, p, pagerState), _scratchPager);
        }

        public byte* AcquirePagePointer(LowLevelTransaction tx, long p)
        {
            return _scratchPager.AcquirePagePointer(tx, p);
        }

        public long ActivelyUsedBytes(long oldestActiveTransaction)
        {
            long result = _allocatedPagesUsedSize;

            var keys = _freePagesByTransaction.Keys;
            var values = _freePagesByTransaction.Values;
            for (int i = 0; i < keys.Count; i++ )
            {
                if (keys[i] < oldestActiveTransaction)
                    break;
                result += values[i];
            }

            return result * _scratchPager.PageSize;
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

        public void BreakLargeAllocationToSeparatePages(PageFromScratchBuffer value)
        {
            if (_allocatedPages.Remove(value.PositionInScratchBuffer) == false)
                throw new InvalidOperationException("Attempt to break up a page that wasn't currently allocated: " +
                                                    value.PositionInScratchBuffer);

            _allocatedPages.Add(value.PositionInScratchBuffer,
                       new PageFromScratchBuffer(value.ScratchFileNumber, value.PositionInScratchBuffer, value.Size, 1));

            for (int i = 1; i < value.NumberOfPages; i++)
            {
                _allocatedPages.Add(value.PositionInScratchBuffer + i,
                    new PageFromScratchBuffer(value.ScratchFileNumber, value.PositionInScratchBuffer + i, 0, 1));
            }
        }
    }
}