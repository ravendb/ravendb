using System.Runtime.InteropServices;

namespace Voron.Impl.FileHeaders
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct TreeRootHeader
    {
        [FieldOffset(0)]
        public long RootPageNumber;
        [FieldOffset(8)]
        public long BranchPages;
        [FieldOffset(16)]
        public long LeafPages;
        [FieldOffset(32)]
        public long OverflowPages;
        [FieldOffset(40)]
        public long PageCount;
        [FieldOffset(48)]
        public long EntriesCount;
        [FieldOffset(56)]
        public int Depth;
        [FieldOffset(60)]
        public TreeFlags Flags;
    }
}