using System.Runtime.InteropServices;

namespace Voron.Data.Sets
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct SetState
    {
        [FieldOffset(0)]
        public RootObjectType RootObjectType;
        [FieldOffset(1)]
        public fixed byte Reserved[3];
        [FieldOffset(4)]
        public int Depth;
        [FieldOffset(8)]
        public long RootPage;
        [FieldOffset(16)]
        public int BranchPages;
        [FieldOffset(20)]
        public int LeafPages;
        [FieldOffset(24)]
        public long NumberOfEntries;

        public override string ToString()
        {
            return $"{nameof(RootObjectType)}: {RootObjectType}, {nameof(Depth)}: {Depth}, {nameof(RootPage)}: {RootPage}, {nameof(BranchPages)}: {BranchPages}, {nameof(LeafPages)}: {LeafPages}";
        }
    }
}
