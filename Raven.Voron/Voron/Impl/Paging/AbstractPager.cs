using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Voron.Trees;

namespace Voron.Impl
{
	public unsafe abstract class AbstractPager : IVirtualPager
	{
		protected int MinIncreaseSize { get { return 16 * PageSize; } }

		private long _increaseSize;
		private DateTime _lastIncrease;
		private IntPtr _tempPage;
		public PagerState PagerState { get; protected set; }

		protected AbstractPager()
		{
			_increaseSize = MinIncreaseSize;
			MaxNodeSize = 1024;
			Debug.Assert((PageSize - Constants.PageHeaderSize) / Constants.MinKeysInPage >= 1024);
			PageMaxSpace = PageSize - Constants.PageHeaderSize;
			PageMinSpace = (int)(PageMaxSpace * 0.33);
			PagerState = new PagerState();
			_tempPage = Marshal.AllocHGlobal(PageSize);
			PagerState.AddRef();
		}

		public int PageMaxSpace { get; private set; }
		public int MaxNodeSize { get; private set; }
		public int PageMinSpace { get; private set; }
		public bool DeleteOnClose { get; set; }

		public int PageSize
		{
			get { return 4096; }
		}

		public long NumberOfAllocatedPages { get; protected set; }

		public Page Read(long pageNumber)
		{
			if (pageNumber + 1 > NumberOfAllocatedPages)
			{
				throw new InvalidOperationException("Cannot increase size of the pager");
			}

			return new Page(AcquirePagePointer(pageNumber), PageMaxSpace);
		}

		public virtual Page GetWritable(long pageNumber)
		{
			if (pageNumber + 1 > NumberOfAllocatedPages)
			{
				throw new InvalidOperationException("Cannot get page number " + pageNumber +
													" because number of allocated pages is " + NumberOfAllocatedPages);
			}

			return new Page(AcquirePagePointer(pageNumber), PageMaxSpace);
		}

		public abstract byte* AcquirePagePointer(long pageNumber);

		public abstract void Flush(long startPage, long count);
		public abstract void Sync();

		public virtual PagerState TransactionBegan()
		{
			var state = PagerState;
			state.AddRef();
			return state;
		}

		public void EnsureContinuous(Transaction tx, long requestedPageNumber, int pageCount)
		{
			if (requestedPageNumber + pageCount <= NumberOfAllocatedPages)
				return;

			// this ensure that if we want to get a range that is more than the current expansion
			// we will increase as much as needed in one shot
			var minRequested = (requestedPageNumber + pageCount) * PageSize;
			var allocationSize = Math.Max(NumberOfAllocatedPages * PageSize, PageSize);
			while (minRequested > allocationSize)
			{
				allocationSize = GetNewLength(allocationSize);
			}

			AllocateMorePages(tx, allocationSize);

		}

		public bool ShouldGoToOverflowPage(int len)
		{
			return len + Constants.PageHeaderSize > MaxNodeSize;
		}

		public int GetNumberOfOverflowPages(int overflowSize)
		{
			overflowSize += Constants.PageHeaderSize;
			return (overflowSize / PageSize) + (overflowSize % PageSize == 0 ? 0 : 1);
		}

		public abstract void Write(Page page, long? pageNumber);

		public bool Disposed { get; private set; }

		public virtual void Dispose()
		{
			if (_tempPage != IntPtr.Zero)
			{
				Marshal.FreeHGlobal(_tempPage);
				_tempPage = IntPtr.Zero;
			}

			Disposed = true;
		}

		public abstract void AllocateMorePages(Transaction tx, long newLength);

		public Page TempPage
		{
			get
			{
				return new Page((byte*)_tempPage.ToPointer(), PageMaxSpace)
				{
					Upper = (ushort)PageSize,
					Lower = (ushort)Constants.PageHeaderSize,
					Flags = 0,
				};
			}
		}

		private long GetNewLength(long current)
		{
			DateTime now = DateTime.UtcNow;
			if (_lastIncrease == DateTime.MinValue)
			{
				_lastIncrease = now;
				return MinIncreaseSize;
			}
			TimeSpan timeSinceLastIncrease = (now - _lastIncrease);
			if (timeSinceLastIncrease.TotalSeconds < 30)
			{
				_increaseSize = Math.Min(_increaseSize * 2, current + current / 4);
			}
			else if (timeSinceLastIncrease.TotalMinutes > 2)
			{
				_increaseSize = Math.Max(MinIncreaseSize, _increaseSize / 2);
			}
			_lastIncrease = now;
			// At any rate, we won't do an increase by over 25% of current size, to prevent huge empty spaces
			// and the first size we allocate is 256 pages (1MB)
			// 
			// The reasoning behind this is that we want to make sure that we increase in size very slowly at first
			// because users tend to be sensitive to a lot of "wasted" space. 
			// We also consider the fact that small increases in small files would probably result in cheaper costs, and as
			// the file size increases, we will reserve more & more from the OS.
			// This also plays avoids "I added 300 records and the file size is 64MB" problems that occur when we are too
			// eager to reserve space
			current = Math.Max(current, 256 * PageSize);
			var actualIncrease = Math.Min(_increaseSize, current / 4);

			return current + actualIncrease;
		}
	}
}
