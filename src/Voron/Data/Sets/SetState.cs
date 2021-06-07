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
        public long NumberOfEntries;
        [FieldOffset(24)]
        public long BranchPages;
        [FieldOffset(32)]
        public long LeafPages;

        public override string ToString()
        {
            return $"{nameof(RootObjectType)}: {RootObjectType}, {nameof(Depth)}: {Depth}, {nameof(RootPage)}: {RootPage}, {nameof(NumberOfEntries)}: {NumberOfEntries}, {nameof(BranchPages)}: {BranchPages}, {nameof(LeafPages)}: {LeafPages}";
        }
    }
}
