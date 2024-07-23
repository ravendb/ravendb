#nullable enable

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Meters;
using Sparrow.Server.Platform;
using Sparrow.Server.Platform.Win32;
using Sparrow.Utils;
using Voron.Exceptions;
using Voron.Platform.Win32;
using Voron.Global;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
#if VALIDATE
    public const bool ProtectPages = true;
#else
    public const bool ProtectPages = false;
#endif

    public static class Bits64
    {
        public const int AllocationGranularity = 64 * Constants.Size.Kilobyte;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long NearestSizeToAllocationGranularity(long size)
        {
            return ((size / AllocationGranularity) + 1) * AllocationGranularity;
        }

        public static Functions CreateFunctions() => new()
        {
            AcquirePagePointer = &AcquirePagePointer,
            AcquireRawPagePointer = &AcquirePagePointer,
            AcquirePagePointerForNewPage = &AcquirePagePointerForNewPage,
            ProtectPageRange = ProtectPages ? &ProtectPageRange : &ProtectPageNoop,
            UnprotectPageRange = ProtectPages ? &UnprotectPageRange : &ProtectPageNoop,
            EnsureMapped = &EnsureMapped,
        };

        private static bool EnsureMapped(Pager pager, State state, ref PagerTransactionState txState, long pageNumber, int numberOfPages)
        {
            _ = pager;
            _ = state;
            _ = txState;
            _ = pageNumber;
            _ = numberOfPages;
            return false;
        }

        public static void ProtectPageNoop(byte* start, ulong size) { }

        public static void ProtectPageRange(byte* start, ulong size)
        {
            var rc = Win32MemoryProtectMethods.VirtualProtect(start, new UIntPtr(size), Win32MemoryProtectMethods.MemoryProtection.READONLY, out _);
            if (!rc)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
        }

        public static void UnprotectPageRange(byte* start, ulong size)
        {
            bool rc = Win32MemoryProtectMethods.VirtualProtect(start, new UIntPtr(size), Win32MemoryProtectMethods.MemoryProtection.READWRITE, out _);
            if (!rc)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
        }

        public static byte* AcquirePagePointerForNewPage(Pager pager, long pageNumber, int numberOfPages, State state, ref PagerTransactionState txState)
        {
            _ = numberOfPages;
            return AcquirePagePointer(pager, state, ref txState, pageNumber);
        }

        public static byte* AcquirePagePointer(Pager pager, State state, ref PagerTransactionState txState, long pageNumber)
        {
            _ = txState;
            if (pageNumber > state.NumberOfAllocatedPages || pageNumber < 0)
                goto InvalidPage;
            if (state.Disposed)
                goto AlreadyDisposed;

            if (pager._canPrefetch && pager._prefetchState.ShouldPrefetchSegment(pageNumber, out long offsetFromFileBase, out long bytes))
            {
                var command = new PalDefinitions.PrefetchRanges(state.BaseAddress + offsetFromFileBase, bytes);
                GlobalPrefetchingBehavior.GlobalPrefetcher.Value.CommandQueue.TryAdd(command, 0);
            }

            return state.BaseAddress + pageNumber * Constants.Storage.PageSize;

            InvalidPage:
            VoronUnrecoverableErrorException.Raise(pager.Options, $"The page {pageNumber} was not allocated in {pager.FileName}");

            AlreadyDisposed:
            throw new ObjectDisposedException("PagerState was already disposed");
        }
    }
}
