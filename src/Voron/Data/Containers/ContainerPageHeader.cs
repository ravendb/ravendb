using System.Runtime.InteropServices;

namespace Voron.Data.Containers
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public unsafe struct ContainerPageHeader
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public ushort NumberOfOffsets;

        [FieldOffset(10)]
        public ushort FloorOfData;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public ExtendedPageType ContainerFlags;

        [FieldOffset(14)] 
        public bool OnFreeList;

        [FieldOffset(15)]
        private readonly byte Reserved;

        [FieldOffset(16)]
        public long PageLevelMetadata;

        [FieldOffset(24)]
        public int NumberOfOverflowPages;

        public const int FreeListOffset = 0;
        public const int AllPagesOffset = 1;
        public const int NumberOfEntriesOffset = 2;
        public const int NextFreePageOffset = 3;

        public int CeilingOfOffsets => NumberOfOffsets * sizeof(Container.ItemMetadata) + PageHeader.SizeOf;
    }
}
