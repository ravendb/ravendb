using System;
using System.Collections.Generic;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl
{
	/// <summary>
	/// This class implements the page pool for in flight transaction information
	/// Pages allocated from here are expected to live after the write transaction that 
	/// created them. The pages will be kept around until the flush for the journals
	/// send them to the data file.
	/// 
	/// This class relies on external syncronization and is not meant to be used in multiple
	/// threads at the same time
	/// </summary>
	public unsafe class ScratchBufferPool : IDisposable
	{
		private readonly IVirtualPager _scratchPager;
		private readonly Dictionary<long, LinkedList<long>> _freePagesBySize = new Dictionary<long, LinkedList<long>>();
		private long _lastUsedPage;

		public ScratchBufferPool(StorageEnvironment env)
		{
			_scratchPager = env.Options.CreateScratchPager();
			_scratchPager.AllocateMorePages(null, env.Options.InitialLogFileSize);
		}

		public PageFromScratchBuffer Allocate(Transaction tx, int numberOfPages)
		{
			var size = Utils.NearestPowerOfTwo(numberOfPages);
			LinkedList<long> list;
			if (_freePagesBySize.TryGetValue(size, out list) && list.Count > 0)
			{
				var position = list.First.Value;
				list.RemoveFirst();
				return new PageFromScratchBuffer
				{
					Pointer = _scratchPager.PagerState.Base + position,
					PositionInScratchBuffer = position,
					Size = size,
					NumberOfPages = numberOfPages
				};
			}
			// we don't have free pages to give out, need to allocate some
			_scratchPager.EnsureContinuous(tx, _lastUsedPage, numberOfPages);

			var result = new PageFromScratchBuffer
			{
				Pointer = _scratchPager.PagerState.Base,
				PositionInScratchBuffer = _lastUsedPage,
				Size = size,
				NumberOfPages = numberOfPages
			};

			_lastUsedPage += numberOfPages;

			return result;
		}

		public void Free(PageFromScratchBuffer page)
		{
			LinkedList<long> list;
			if (_freePagesBySize.TryGetValue(page.Size, out list) == false)
			{
				list = new LinkedList<long>();
				_freePagesBySize[page.Size] = list;
			}
			list.AddFirst(page.PositionInScratchBuffer);
		}

		public void Dispose()
		{
			_scratchPager.Dispose();
		}

		public Page ReadPage(long value)
		{
			return _scratchPager.Read(value);
		}
	}

	public unsafe class PageFromScratchBuffer
	{
		public byte* Pointer;
		public long PositionInScratchBuffer;
		public long Size;
		public int NumberOfPages;
	}
}