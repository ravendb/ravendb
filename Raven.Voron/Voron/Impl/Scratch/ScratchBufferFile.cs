// -----------------------------------------------------------------------
//  <copyright file="ScratchFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
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
			public long ValidAfterTransactionId;
		}

		private readonly IVirtualPager _scratchPager;
		private readonly int _scratchNumber;
		private readonly Dictionary<long, LinkedList<PendingPage>> _freePagesBySize = new Dictionary<long, LinkedList<PendingPage>>();
		private readonly Dictionary<long, PageFromScratchBuffer> _allocatedPages = new Dictionary<long, PageFromScratchBuffer>();
		private long _lastUsedPage;

		public ScratchBufferFile(IVirtualPager scratchPager, int scratchNumber)
		{
			_scratchPager = scratchPager;
			_scratchNumber = scratchNumber;
		}

		public PagerState PagerState { get { return _scratchPager.PagerState; } }

		public int NumberOfAllocations
		{
			get { return _allocatedPages.Count; }
		}

		public long Size
		{
			get { return _scratchPager.NumberOfAllocatedPages*AbstractPager.PageSize; }
		}

		public long SizeAfterAllocation(long sizeToAllocate)
		{
			return (_lastUsedPage + sizeToAllocate) * AbstractPager.PageSize;
		}

		public PageFromScratchBuffer Allocate(Transaction tx, int numberOfPages, long size)
		{
			_scratchPager.EnsureContinuous(tx, _lastUsedPage, (int) size);

			var result = new PageFromScratchBuffer
			{
				ScratchFileNumber = _scratchNumber,
				PositionInScratchBuffer = _lastUsedPage,
				Size = size,
				NumberOfPages = numberOfPages
			};

			_allocatedPages.Add(_lastUsedPage, result);
			_lastUsedPage += size;

			return result;
		}

		public bool HasDiscontinuousSpaceFor(Transaction tx, long size)
		{
			var sizesFromLargest = _freePagesBySize.Keys.OrderByDescending(x => x).ToList();

			var oldestTransaction = tx.Environment.OldestTransaction;
			long available = 0;

			foreach (var sizeKey in sizesFromLargest)
			{
				var item = _freePagesBySize[sizeKey].Last;
				while (oldestTransaction == 0 || item.Value.ValidAfterTransactionId < oldestTransaction)
				{
					available += sizeKey;

					if(available >= size || item.Previous == null)
						break;

					item = item.Previous;
				}

				if(available >= size)
					break;
			}

			return available >= size;
		}

		public bool TryGettingFromAllocatedBuffer(Transaction tx, int numberOfPages, long size, out PageFromScratchBuffer result)
		{
			result = null;
			LinkedList<PendingPage> list;
			if (!_freePagesBySize.TryGetValue(size, out list) || list.Count <= 0)
				return false;
			var val = list.Last.Value;
			var oldestTransaction = tx.Environment.OldestTransaction;
			if (oldestTransaction != 0 && val.ValidAfterTransactionId >= oldestTransaction) // OldestTransaction can be 0 when there are none other transactions and we are in process of new transaction header allocation
				return false;

			list.RemoveLast();
			var pageFromScratchBuffer = new PageFromScratchBuffer
			{
				ScratchFileNumber = _scratchNumber,
				PositionInScratchBuffer = val.Page,
				Size = size,
				NumberOfPages = numberOfPages
			};

			_allocatedPages.Add(val.Page, pageFromScratchBuffer);
			{
				result = pageFromScratchBuffer;
				return true;
			}
		}

		public void Free(long page, long asOfTxId)
		{
			PageFromScratchBuffer value;
			if (_allocatedPages.TryGetValue(page, out value) == false)
			{
				throw new InvalidOperationException("Attempt to free page that wasn't currently allocated: " + page);
			}
			_allocatedPages.Remove(page);
			LinkedList<PendingPage> list;
			if (_freePagesBySize.TryGetValue(value.Size, out list) == false)
			{
				list = new LinkedList<PendingPage>();
				_freePagesBySize[value.Size] = list;
			}
			list.AddFirst(new PendingPage
			{
				Page = value.PositionInScratchBuffer,
				ValidAfterTransactionId = asOfTxId
			});
		}

		public Page ReadPage(long p, PagerState pagerState = null)
		{
			return _scratchPager.Read(p, pagerState);
		}

		public byte* AcquirePagePointer(long p)
		{
			return _scratchPager.AcquirePagePointer(p);
		}

		public long ActivelyUsedBytes(long oldestActiveTransaction)
		{
			long result = _allocatedPages.Sum(allocation => allocation.Value.Size);

			foreach (var free in _freePagesBySize)
			{
				var item = free.Value.First;

				while (item.Value.ValidAfterTransactionId >= oldestActiveTransaction)
				{
					result += free.Key;

					if(item.Next == null)
						break;
				}
			}

			return result * AbstractPager.PageSize;
		}

		public void Dispose()
		{
			_scratchPager.Dispose();
		}
	}
}