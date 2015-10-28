using System.Runtime.InteropServices;

namespace Voron.Data.BTrees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct TreePageHeader
	{
		[FieldOffset(0)]
		public long PageNumber;

        [FieldOffset(8)]
        public int OverflowSize;

        [FieldOffset(12)]
        public PageFlags Flags;

		[FieldOffset(13)]
		public TreePageFlags TreeFlags;

		[FieldOffset(14)]
		public ushort Lower;

		[FieldOffset(16)]
		public ushort Upper;
	
		[FieldOffset(8)]
		public ushort FixedSize_StartPosition;

        [FieldOffset(10)]
        public ushort FixedSize_NumberOfEntries;
        
        [FieldOffset(14)]
        public ushort FixedSize_ValueSize;

	}

    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct FixedSizeTreePageHeader
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public ushort StartPosition;

        [FieldOffset(10)]
        public ushort NumberOfEntries;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public FixedSizeTreePageFlags TreeFlags;

        [FieldOffset(14)]
        public ushort ValueSize;
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct RawDataSmallSectionPageHeader
    {
        public const int NumberOfPagesInSmallSection = 128;

        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public ushort NumberOfEntries;

        [FieldOffset(10)]
        public ushort NextAllocation;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public RawDataPageFlags RawDataFlags;

        [FieldOffset(14)] public int NumberOfEntriesInSection;

        [FieldOffset(18)]
        public fixed ushort AvailableSpace [NumberOfPagesInSmallSection];
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct RawDataSmallPageHeader
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public ushort NumberOfEntries;

        [FieldOffset(10)]
        public ushort NextAllocation;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public RawDataPageFlags RawDataFlags;
    }
}