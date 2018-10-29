using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Utils;
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
        private Logger _log = LoggingSource.Instance.GetLogger<PosixAbstractPager>("PosixAbstractPager");

        public override int CopyPage(I4KbBatchWrites destwI4KbBatchWrites, long p, PagerState pagerState)
        {
            return CopyPageImpl(destwI4KbBatchWrites, p, pagerState);
        }

        private unsafe void PrefetchRanges(List<Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY> ranges)
        {
            foreach (var range in ranges)
            {
                if (Syscall.madvise(new IntPtr(range.VirtualAddress), (UIntPtr)range.NumberOfBytes.ToPointer(), MAdvFlags.MADV_WILLNEED) == -1)
                {
                    // ignore this error.
                }
            }
        }

        public override unsafe void TryPrefetchingWholeFile()
        {
            if (PlatformDetails.CanPrefetch == false)
                return; // not supported

            long size = 0;
            void* baseAddress = null;
            var pagerState = PagerState;
            var range = new List<Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY>(pagerState.AllocationInfos.Length);
            for (int i = 0; i < pagerState.AllocationInfos.Length; i++)
            {
                var allocInfo = pagerState.AllocationInfos[i];
                size += allocInfo.Size;

                if (baseAddress == null)
                    baseAddress = allocInfo.BaseAddress;

                if (i != pagerState.AllocationInfos.Length - 1 &&
                    pagerState.AllocationInfos[i + 1].BaseAddress == allocInfo.BaseAddress + allocInfo.Size)
                {
                    continue; // if adjacent ranges make one syscall
                }

                range.Add(new Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY
                {
                    VirtualAddress = baseAddress,
                    NumberOfBytes = new IntPtr(size)
                });

                size = 0;
                baseAddress = null;
            }
            PrefetchRanges(range);
        }

        public override unsafe void MaybePrefetchMemory(List<long> pagesToPrefetch)
        {
            if (PlatformDetails.CanPrefetch == false)
                return; // not supported

            if (pagesToPrefetch.Count == 0)
                return;

            long sizeToPrefetch = 4 * Constants.Storage.PageSize;

            long size = 0;
            void* baseAddress = null;
            var range = new List<Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY>(pagesToPrefetch.Count);
            for (int i = 0; i < pagesToPrefetch.Count; i++)
            {
                var addressToPrefetch = pagesToPrefetch[i];
                size += sizeToPrefetch;

                if (baseAddress == null)
                    baseAddress = (void*)addressToPrefetch;

                if (i != pagesToPrefetch.Count - 1 &&
                    pagesToPrefetch[i + 1] == addressToPrefetch + sizeToPrefetch)
                {
                    continue; // if adjacent ranges make one syscall
                }

                range.Add(new Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY
                {
                    VirtualAddress = baseAddress,
                    NumberOfBytes = new IntPtr(size)
                });

                size = 0;
                baseAddress = null;
            }
            PrefetchRanges(range);
        }

        protected PosixAbstractPager(StorageEnvironmentOptions options, bool usePageProtection = false) : base(options, usePageProtection: usePageProtection)
        {
        }

        protected unsafe void ReleaseAllocationInfoWithoutUnmapping(byte* baseAddress, long size)
        {
            // should be called from Posix32BitsMemoryMapPager in order to bypass unmapping
            base.ReleaseAllocationInfo(baseAddress, size);
        }
        
        public override unsafe void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            // 32 bits should override this method and call AbstractPager::ReleaseAllocationInfo
            base.ReleaseAllocationInfo(baseAddress, size);
            var ptr = new IntPtr(baseAddress);

            if (DeleteOnClose)
            {
                if (Syscall.madvise(ptr, new UIntPtr((ulong)size), MAdvFlags.MADV_DONTNEED) != 0)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Failed to madvise MDV_DONTNEED for {FileName?.FullPath}");
                }
            }
            
            var result = Syscall.munmap(ptr, (UIntPtr)size);
            if (result == -1)
            {
                var err = Marshal.GetLastWin32Error();
                Syscall.ThrowLastError(err, "munmap " + FileName);
            }
            NativeMemory.UnregisterFileMapping(FileName.FullPath, ptr, size);
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
