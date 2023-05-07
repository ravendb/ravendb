using System;
using System.Runtime.InteropServices;

namespace Voron.Data.Lookups
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct LookupState
    {
        [FieldOffset(0)]
        public RootObjectType RootObjectType;
        [FieldOffset(1)]
        public fixed byte Reserved[7];
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
            return $"{nameof(RootObjectType)}: {RootObjectType}, {nameof(RootPage)}: {RootPage}, {nameof(NumberOfEntries)}: {NumberOfEntries:#,#}, {nameof(BranchPages)}: {BranchPages:#,#}, {nameof(LeafPages)}: {LeafPages:#,#}";
        }

        public void CopyTo(LookupState* header)
        {
            header->RootObjectType = RootObjectType;
            header->RootPage = RootPage;
            header->NumberOfEntries = NumberOfEntries;
            header->BranchPages = BranchPages;
            header->LeafPages = LeafPages;
        }
    }
}
