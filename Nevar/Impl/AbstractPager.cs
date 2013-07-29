using System;
using Nevar.Trees;

namespace Nevar.Impl
{
	public abstract class AbstractPager : IVirtualPager
	{
		protected const int MinIncreaseSize = 1024 * 1024 * 4;
		protected const int MaxIncreaseSize = 1024 * 1024 * 1024;

		private int _increaseSize = MinIncreaseSize;
		private DateTime _lastIncrease;
		protected PagerState _pagerState;

		protected AbstractPager()
		{
			MaxNodeSize = (PageSize - Constants.PageHeaderSize) / Constants.MinKeysInPage;
			PageMaxSpace = PageSize - Constants.PageHeaderSize;
			PageMinSpace = (int)(PageMaxSpace * 0.33);
		}

		public int PageMaxSpace { get; private set; }
		public int MaxNodeSize { get; private set; }
		public int PageMinSpace { get; private set; }

		public int PageSize
		{
			get { return 4096; }
		}

		public abstract long NumberOfAllocatedPages { get; }

		public abstract Page Get(long n);

		public abstract void Flush();

		public virtual PagerState TransactionBegan()
		{
			var state = _pagerState;
			state.AddRef();
			return state;
		}

		public virtual void TransactionCompleted(PagerState state)
		{
			_pagerState.Release();
		}

		public virtual void EnsureContinious(long requestedPageNumber, int pageCount)
		{
			if (requestedPageNumber + pageCount < NumberOfAllocatedPages)
				return;

			// this ensure that if we want to get a range that is more than the current expansion
			// we will increase as much as needed in one shot
			var minRequested = (requestedPageNumber + pageCount) * PageSize;
			var allocationSize = NumberOfAllocatedPages * PageSize;
			while (minRequested > allocationSize)
			{
				allocationSize = GetNewLength(allocationSize);
			}

			AllocateMorePages(allocationSize);
		}

		public abstract void Dispose();
		protected abstract void AllocateMorePages(long newLength);


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
			_lastIncrease = DateTime.UtcNow;
			return current + _increaseSize;
		}
	}
}