using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Platform.Win32;

namespace Voron.Platform.Posix
{
    public abstract class PosixAbstractPager : AbstractPager
    {
        internal int _fd;

        public override int CopyPage(I4KbBatchWrites destwI4KbBatchWrites, long p, PagerState pagerState)
        {
            return CopyPageImpl(destwI4KbBatchWrites, p, pagerState);
        }

        protected internal override unsafe void PrefetchRanges(Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY* list, int count)
        {
            for (int i = 0; i < count; i++)
            {
                // we explicitly ignore the return code here, this is optimization only
                Syscall.madvise(new IntPtr(list[i].VirtualAddress), (UIntPtr)list[i].NumberOfBytes.ToPointer(), MAdvFlags.MADV_WILLNEED);
            }
        }

        public override unsafe byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            // We need to decide what pager we are going to use right now or risk inconsistencies when performing prefetches from disk.
            var state = pagerState ?? _pagerState;

            if (PlatformDetails.CanPrefetch)
            {
                if (this._pagerState.ShouldPrefetchSegment(pageNumber, out void* virtualAddress, out long bytes))
                {
                    Syscall.madvise(new IntPtr(virtualAddress), (UIntPtr)bytes, MAdvFlags.MADV_WILLNEED);
                }
            }

            return base.AcquirePagePointer(tx, pageNumber, state);
        }

        protected PosixAbstractPager(StorageEnvironmentOptions options, bool canPrefetchAhead, bool usePageProtection = false) : base(options, canPrefetchAhead, usePageProtection: usePageProtection)
        {
        }
        
        protected override void DisposeInternal()
        {
            if (_fd != -1)
            {
                // note that the orders of operations is important here, we first unlink the file
                // we are supposed to be the only one using it, so Linux would be ready to delete it
                // and hopefully when we close it, won't waste any time trying to sync the memory state
                // to disk just to discard it
                if (DeleteOnClose)
                {
                    Syscall.unlink(FileName.FullPath);
                    // explicitly ignoring the result here, there isn't
                    // much we can do to recover from being unable to delete it
                }
                Syscall.close(_fd);
                _fd = -1;
            }
        }
    }
}
