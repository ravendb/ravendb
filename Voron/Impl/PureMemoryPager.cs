namespace Voron.Impl
{
	using System;
	using System.Collections.Generic;
	using System.Runtime.InteropServices;

	public unsafe class PureMemoryPager : AbstractPager
	{
		private IntPtr _ptr;
		private long _allocatedSize;
		private byte* _base;

		public PureMemoryPager(byte[] data)
		{
			_ptr = Marshal.AllocHGlobal(data.Length);
			_base = (byte*)_ptr.ToPointer();
			NumberOfAllocatedPages = data.Length / PageSize;
			PagerState.Release();
			PagerState = new PagerState
			{
				Ptr = _ptr,
				Base = _base
			};
			PagerState.AddRef();
			fixed (byte* origin = data)
			{
				NativeMethods.memcpy(_base, origin, data.Length);
			}
		}

		public PureMemoryPager()
		{
			_ptr = Marshal.AllocHGlobal(MinIncreaseSize);
			_base = (byte*)_ptr.ToPointer();
			NumberOfAllocatedPages = _allocatedSize / PageSize;
			PagerState.Release();
			PagerState = new PagerState
			{
				Ptr = _ptr,
				Base = _base
			};
			PagerState.AddRef();
		}

		public override void EnsureEnoughSpace(Transaction tx, int len)
		{
			var pages = 10; // TODO [ppekrol] When using PureMemoryPager, all memory allocations are causing issues with writes (contex is working on 'old' memory) - this will be fixed after LogFile is introduced by Arek.
			if (ShouldGoToOverflowPage(len))
				pages = GetNumberOfOverflowPages(tx, len);

			EnsureContinious(tx, tx.NextPageNumber, pages);
		}

		public override void Dispose()
		{
			base.Dispose();
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

		public override void AllocateMorePages(Transaction tx, long newLength)
		{
			if (newLength <= _allocatedSize)
				throw new ArgumentException("Cannot set the legnth to less than the current length");

			var oldSize = _allocatedSize;
			_allocatedSize = newLength;
			NumberOfAllocatedPages = _allocatedSize / PageSize;
			var newPtr = Marshal.AllocHGlobal(new IntPtr(_allocatedSize));
			var newBase = (byte*)newPtr.ToPointer();
			NativeMethods.memcpy(newBase, _base, new IntPtr(oldSize));
			_base = newBase;
			_ptr = newPtr;

			PagerState.Release(); // when the last transaction using this is over, will dispose it
			PagerState newPager = CreateNewPagerState(newBase, newPtr);

			if (tx != null) // we only pass null during startup, and we don't need it there
			{
				newPager.AddRef(); // one for the current transaction
				tx.AddPagerState(newPager);
			}

			PagerState = newPager;
		}

		private PagerState CreateNewPagerState(byte* newBase, IntPtr newPtr)
		{
			var newPager = new PagerState
			{
				Base = newBase,
				Ptr = newPtr
			};
			newPager.AddRef(); // one for the pager
			return newPager;
		}
	}
}
