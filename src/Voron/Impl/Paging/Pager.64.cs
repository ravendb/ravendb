#nullable enable

using System;
using System.Runtime.CompilerServices;
using Sparrow.Server.Platform;
using Voron.Exceptions;
using Voron.Global;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{

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
            EnsureMapped = null,
            ConvertToWritePointer = &ConvertToWritePointer
        };

        private static byte* ConvertToWritePointer(Pager pager, State state, byte* ptr)
        {
            return state.WriteAddress + (ptr - state.ReadAddress);
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
                var command = new PalDefinitions.PrefetchRanges(state.ReadAddress + offsetFromFileBase, bytes);
                GlobalPrefetchingBehavior.GlobalPrefetcher.Value.CommandQueue.TryAdd(command, 0);
            }

            return state.ReadAddress + pageNumber * Constants.Storage.PageSize;

            InvalidPage:
            VoronUnrecoverableErrorException.Raise(pager.Options, $"The page {pageNumber} was not allocated in {pager.FileName}");

            AlreadyDisposed:
            throw new ObjectDisposedException("PagerState was already disposed");
        }
    }
}
