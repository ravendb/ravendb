namespace Voron.Impl
{
	using System;
	using System.Collections.Generic;
	using System.Runtime.InteropServices;

	using Voron.Trees;

	public unsafe abstract class AbstractPager : IVirtualPager
	{
		protected const int MinIncreaseSize = 1 * 1024 * 1024;
		protected const int MaxIncreaseSize = 256 * 1024 * 1024;

		private long _increaseSize = MinIncreaseSize;
		private DateTime _lastIncrease;
		private IntPtr _tempPage;
		public PagerState PagerState { get; protected set; }

		protected AbstractPager()
		{
			MaxNodeSize = (PageSize - Constants.PageHeaderSize) / Constants.MinKeysInPage;
			PageMaxSpace = PageSize - Constants.PageHeaderSize;
			PageMinSpace = (int)(PageMaxSpace * 0.33);
			PagerState = new PagerState();
			_tempPage = Marshal.AllocHGlobal(PageSize);
			PagerState.AddRef();
		}

		public int PageMaxSpace { get; private set; }
		public int MaxNodeSize { get; private set; }
		public int PageMinSpace { get; private set; }

		public int PageSize
		{
			get { return 4096; }
		}

		public long NumberOfAllocatedPages { get; protected set; }

		public Page Get(Transaction tx, long n, bool errorOnChange = false)
		{
			if (n + 1 > NumberOfAllocatedPages && errorOnChange)
			{
				throw new InvalidOperationException("Cannot increase size of the pager when errorOnChange is set to true");
			}
			EnsureContinious(tx, n, 1);
			var pagerState = tx == null || tx.LatestPagerState == null ? PagerState : tx.LatestPagerState;
			return Get(pagerState, n);
		}

		protected Page Get(PagerState txState, long n)
		{
			return new Page(txState.Base + (n * PageSize), PageMaxSpace);
		}

		public abstract void Flush(List<long> sortedPagesToFlush);
		public abstract void Flush(long headerPageId);
		public abstract void Sync();

		public virtual PagerState TransactionBegan()
		{
			var state = PagerState;
			state.AddRef();
			return state;
		}

		public virtual void EnsureContinious(Transaction tx, long requestedPageNumber, int pageCount)
		{
			if (requestedPageNumber + pageCount <= NumberOfAllocatedPages)
				return;

			// this ensure that if we want to get a range that is more than the current expansion
			// we will increase as much as needed in one shot
			var minRequested = (requestedPageNumber + pageCount) * PageSize;
			var allocationSize = NumberOfAllocatedPages * PageSize;
			while (minRequested > allocationSize)
			{
				allocationSize = GetNewLength(allocationSize);
			}

			AllocateMorePages(tx, allocationSize);
		}

		public abstract void EnsureEnoughSpace(Transaction tx, int len);

		public virtual void Dispose()
		{
			if (_tempPage != IntPtr.Zero)
			{
				Marshal.FreeHGlobal(_tempPage);
				_tempPage = IntPtr.Zero;
			}
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
			TimeSpan timeSinceLastIncrease = (now - _lastIncrease);
			if (timeSinceLastIncrease.TotalSeconds < 30)
			{
				_increaseSize = Math.Min(_increaseSize * 2, MaxIncreaseSize);
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

		public bool ShouldGoToOverflowPage(int len)
		{
			return len + Constants.PageHeaderSize > MaxNodeSize;
		}

		public int GetNumberOfOverflowPages(Transaction tx, int overflowSize)
		{
			return (tx.Environment.PageSize - 1 + overflowSize) / (tx.Environment.PageSize) + 1;
		}
	}
}
