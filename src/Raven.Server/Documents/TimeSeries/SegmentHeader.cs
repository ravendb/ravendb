using System.Runtime.InteropServices;

namespace Raven.Server.Documents.TimeSeries
{
    [StructLayout(LayoutKind.Explicit, Size = 16)]
    public unsafe struct SegmentHeader
    {
        [FieldOffset(0)]
        public int PreviousTimeStamp;
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
        public ushort CompressedSize;
    }
}
