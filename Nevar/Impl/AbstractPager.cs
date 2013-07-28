using System;
using Nevar.Trees;

namespace Nevar.Impl
{
	public abstract class AbstractPager : IVirtualPager
	{
		protected const int MinIncreaseSize = 1024*1024*4;
		protected const int MaxIncreaseSize = 1024*1024*1024;

		private int _increaseSize = MinIncreaseSize;
		private DateTime _lastIncrease;

		protected AbstractPager()
		{
			MaxNodeSize = (PageSize - Constants.PageHeaderSize)/Constants.MinKeysInPage;
			PageMaxSpace = PageSize - Constants.PageHeaderSize;
			PageMinSpace = (int) (PageMaxSpace*0.33);
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

		public abstract PagerState TransactionBegan();

		public abstract void TransactionCompleted(PagerState state);

		public virtual void EnsureContinious(long requestedPageNumber, int pageCount)
		{
			for (int i = 0; i < pageCount; i++)
			{
				EnsurePageExists(requestedPageNumber + i);
			}
		}

		public abstract void Dispose();
		protected abstract void EnsurePageExists(long n);


		protected long GetNewLength(long current)
		{
			DateTime now = DateTime.UtcNow;
			TimeSpan timeSinceLastIncrease = (now - _lastIncrease);
			if (timeSinceLastIncrease.TotalSeconds < 30)
			{
				_increaseSize = Math.Min(_increaseSize*2, MaxIncreaseSize);
			}
			else if (timeSinceLastIncrease.TotalMinutes > 2)
			{
				_increaseSize = Math.Max(MinIncreaseSize, _increaseSize/2);
			}
			_lastIncrease = DateTime.UtcNow;
			return current + _increaseSize;
		}
	}
}