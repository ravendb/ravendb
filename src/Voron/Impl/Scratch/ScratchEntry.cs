using Voron.Impl.Paging;

namespace Voron.Impl.Scratch
{
    internal struct ScratchEntry
    {
        internal long PositionInScratchBuffer;
        internal long PageNumberInDataFile;
        internal long AllocatedInTransaction;
        internal long Size;
        internal Page PreviousVersion;
        internal int NumberOfPages;
        internal int RefIndex;
        internal int OlderIndex;
        internal long Seq;

        internal readonly bool IsRemoved => RefIndex < 0;
    }

    internal struct ScratchRef
    {
        internal ScratchBufferFile File;
        internal Pager.State State;
    }
}
