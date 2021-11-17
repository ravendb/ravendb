using System.Runtime.InteropServices;

namespace Raven.Server.Documents.TimeSeries
{
    [StructLayout(LayoutKind.Explicit, Size = 16)]
    public unsafe struct SegmentHeader
    {
        [FieldOffset(0)]
        public int PreviousTimestamp;
        [FieldOffset(4)]
        public int PreviousDelta;
        [FieldOffset(8)]
        public ushort NumberOfEntries;
        [FieldOffset(10)]
        public ushort SizeOfTags;
        [FieldOffset(12)]
        public byte PreviousTagIndex;
        [FieldOffset(13)]
        public byte NumberOfValues;
        [FieldOffset(14)]
        public SegmentVersion Version;
        [FieldOffset(15)]
        public fixed byte Reserved[1];
    }

    public enum SegmentVersion : byte
    {
        V50000 = 0,
        Baseline = 1,
        ContainDuplicates = 2, // segment contain duplicates
        DuplicateLast = 3 // last value in the segment is a duplicate
    }
}
