using System;
using Sparrow;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Voron.Impl.Paging;

namespace Voron.Impl.Scratch
{
    public sealed class PageFromScratchBufferEqualityComparer : IEqualityComparer<PageFromScratchBuffer>
    {
        public static readonly PageFromScratchBufferEqualityComparer Instance = new PageFromScratchBufferEqualityComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(PageFromScratchBuffer x, PageFromScratchBuffer y)
        {
            if (x == y) return true;
            if (x == null || y == null) return false;            

            return x.PositionInScratchBuffer == y.PositionInScratchBuffer && x.NumberOfPages == y.NumberOfPages && x.File == y.File;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(PageFromScratchBuffer obj)
        {
            int v = Hashing.Combine(obj.NumberOfPages, obj.File.Number);
            int w = Hashing.Combine(obj.PositionInScratchBuffer.GetHashCode(), obj.PageNumberInDataFile.GetHashCode());
            return Hashing.Combine(v, w);
        }
    }


    public sealed record PageFromScratchBuffer(
        ScratchBufferFile File,
        Pager.State State,
        long AllocatedInTransaction,
        long PositionInScratchBuffer,
        long PageNumberInDataFile,
        Page PreviousVersion,
        int NumberOfPages,
        bool IsDeleted = false
    )
    {
        public unsafe Page ReadPage(LowLevelTransaction tx)
        {
            return new Page(Read(ref tx.PagerTransactionState));
        }
        
        public unsafe byte* Read(ref Pager.PagerTransactionState txState)
        {
            if (IsDeleted == false)
            {
                File.VerifyMatch(PageNumberInDataFile, PositionInScratchBuffer, NumberOfPages);
                return File.Pager.AcquirePagePointerWithOverflowHandling(State, ref txState, PositionInScratchBuffer);
            }
            throw new InvalidOperationException($"Attempt to read page {PageNumberInDataFile} that was deleted");
        }

        public unsafe Page ReadNewPage(LowLevelTransaction tx)
        {
            var p = File.Pager.AcquirePagePointerForNewPage(State, ref tx.PagerTransactionState, PositionInScratchBuffer, NumberOfPages);
            p = File.Pager.MakeWritable(State, p);
            return new Page(p);
        }

        public unsafe Page ReadRawPage(LowLevelTransaction tx)
        {
            return new Page(ReadRaw(ref tx.PagerTransactionState));
        }
        
        public unsafe byte* ReadRaw(ref Pager.PagerTransactionState txState)
        {
            if (IsDeleted == false)
            {
                File.VerifyMatch(PageNumberInDataFile, PositionInScratchBuffer, NumberOfPages);
                return File.Pager.AcquireRawPagePointerWithOverflowHandling(State, ref txState, PositionInScratchBuffer);
            }
            throw new InvalidOperationException($"Attempt to read page {PageNumberInDataFile} that was deleted");
        }

        public unsafe Page ReadWritable(LowLevelTransaction tx)
        {
            return new Page(ReadWritable(ref tx.PagerTransactionState));
        }

        public unsafe byte* ReadWritable(ref Pager.PagerTransactionState txPagerTransactionState)
        {
            var ptr = Read(ref txPagerTransactionState);
            return File.Pager.MakeWritable(State, ptr);
        }
    }
}
