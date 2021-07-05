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

        // this is used only by the first page
        [FieldOffset(14)] 
        public int NumberOfPages;

        [FieldOffset(18)]
        public int NumberOfOverflowPages;

        [FieldOffset(22)]
        public long NextFreePage;

        [FieldOffset(30)]
        public bool OnFreeList;

        public const int FreeListOffset = 0;
        public const int AllPagesOffset = 1;

        public int CeilingOfOffsets => NumberOfOffsets * sizeof(Container.ItemMetadata) + PageHeader.SizeOf;
    }
}
