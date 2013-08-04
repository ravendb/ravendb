using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Nevar.Trees;

namespace Nevar.Impl
{
	public unsafe class PureMemoryPager : AbstractPager
	{
		private IntPtr _ptr;
		private long _allocatedSize;
		private byte* _base;
		private long _allocatedPages;

		public PureMemoryPager()
		{
			_ptr = Marshal.AllocHGlobal(MinIncreaseSize);
			_base = (byte*)_ptr.ToPointer();
			_allocatedPages = _allocatedSize / PageSize;
			PagerState = new PagerState
				{
					Ptr = _ptr
				};
			PagerState.AddRef();
		}


		public override long NumberOfAllocatedPages
		{
			get { return _allocatedPages; }
		}



		public override void Dispose()
		{
			PagerState.Release();
			_base = null;
		}

		public override void Flush(List<long> sortedPagesToFlush)
		{
			//nothing to do here
		}

		public override void Flush(long headerPageId)
		{
			// also nothing to do
		}

		public override void Sync()
		{
			// nothing to do here
		}

		protected override Page Get(long n)
		{
			return new Page(_base + (n * PageSize), PageMaxSpace);
		}

		public override void AllocateMorePages(Transaction tx, long newLength)
		{
			if (newLength <= _allocatedSize)
				throw new ArgumentException("Cannot set the legnth to less than the current length");

			var oldSize = _allocatedSize;
			_allocatedSize = newLength;
			_allocatedPages = _allocatedSize / PageSize;
			var newPtr = Marshal.AllocHGlobal(new IntPtr(_allocatedSize));
			var newBase = (byte*)newPtr.ToPointer();
			NativeMethods.memcpy(newBase, _base, new IntPtr(oldSize));
			_base = newBase;
			_ptr = newPtr;

			var oldPager = PagerState;

			var newPager = new PagerState { Ptr = newPtr };
			newPager.AddRef(); // one for the pager

			if (tx != null) // we only pass null during startup, and we don't need it there
			{
				newPager.AddRef(); // one for the current transaction
				tx.AddPagerState(PagerState);
			}

			PagerState = newPager;
			oldPager.Release();
		}
	}
}