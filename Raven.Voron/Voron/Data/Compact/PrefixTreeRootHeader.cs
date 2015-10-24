using System.Runtime.InteropServices;

namespace Voron.Data.Compact
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct PrefixTreeRootHeader
    {
        [FieldOffset(0)]
        public long RootPageNumber;
        [FieldOffset(8)]
        public long BranchPages;
        [FieldOffset(16)]
        public long LeafPages;
        [FieldOffset(40)]
        public long PageCount;
        [FieldOffset(48)]
        public long NumberOfEntries;
        [FieldOffset(56)]
        public int Depth;
        [FieldOffset(60)]
        public PrefixTreeFlags Flags;
    }
}
