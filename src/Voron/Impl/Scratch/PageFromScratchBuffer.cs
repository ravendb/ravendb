using System.Runtime.CompilerServices;
using Voron.Impl.Paging;

namespace Voron.Impl.Scratch
{
    public readonly record struct PageFromScratchBuffer(
        ScratchBufferFile File,
        Pager.State State,
        long AllocatedInTransaction,
        long PositionInScratchBuffer,
        long PageNumberInDataFile,
        Page PreviousVersion,
        long Size,
        int NumberOfPages
    )
    {
        internal const long TombstoneTx = -1;
        internal const long SurvivingTombstoneTx = -2;

        public bool IsValid => File != null;

        public unsafe Page ReadPage(LowLevelTransaction tx)
        {
            return new Page(Read(ref tx.PagerTransactionState));
        }

        public unsafe byte* Read(ref Pager.PagerTransactionState txState)
        {
            File.VerifyMatch(PageNumberInDataFile, PositionInScratchBuffer, NumberOfPages);
            return File.Pager.AcquirePagePointerWithOverflowHandling(State, ref txState, PositionInScratchBuffer);
        }

        public unsafe byte* ReadRawPagePointer(ref Pager.PagerTransactionState txState)
        {
            File.VerifyMatch(PageNumberInDataFile, PositionInScratchBuffer, NumberOfPages);
            return File.Pager.AcquireRawPagePointer(State, ref txState, PositionInScratchBuffer);
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
            File.VerifyMatch(PageNumberInDataFile, PositionInScratchBuffer, NumberOfPages);
            return File.Pager.AcquireRawPagePointerWithOverflowHandling(State, ref txState, PositionInScratchBuffer);
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

        public unsafe byte* ReadWritableRawPagePointer(ref Pager.PagerTransactionState txPagerTransactionState)
        {
            var ptr = ReadRawPagePointer(ref txPagerTransactionState);
            return File.Pager.MakeWritable(State, ptr);
        }
    }
}
