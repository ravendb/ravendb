using System;
using System.Runtime.InteropServices;

namespace Voron.Data.CompactTrees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct CompactTreeState
    {
        [FieldOffset(0)]
        public RootObjectType RootObjectType;
        [FieldOffset(1)]
        public CompactTreeFlags Flags;
        [FieldOffset(2)]
        public fixed byte Reserved[6];
        [FieldOffset(8)]
        public long RootPage;
        [FieldOffset(16)]
        public long NumberOfEntries;
        [FieldOffset(24)]
        public long BranchPages;
        [FieldOffset(32)]
        public long LeafPages;
        [FieldOffset(40)]
        public long TreeDictionaryId;
        [FieldOffset(48)]
        public long NextTrainAt;

        public override string ToString()
        {
            return $"{nameof(RootObjectType)}: {RootObjectType}, {nameof(Flags)}: {Flags}, {nameof(RootPage)}: {RootPage}, {nameof(NumberOfEntries)}: {NumberOfEntries}, {nameof(BranchPages)}: {BranchPages}, {nameof(LeafPages)}: {LeafPages}, {nameof(TreeDictionaryId)}: {TreeDictionaryId}";
        }

        public void CopyTo(CompactTreeState* header)
        {
            header->RootObjectType = RootObjectType;
            header->Flags = Flags;
            header->RootPage = RootPage;
            header->NumberOfEntries = NumberOfEntries;
            header->BranchPages = BranchPages;
            header->LeafPages = LeafPages;
            header->TreeDictionaryId = TreeDictionaryId;
            header->NextTrainAt = NextTrainAt;
        }
    }
}
