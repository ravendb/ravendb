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
        public ushort PreviousTagPosition;
        [FieldOffset(12)]
        public byte NumberOfValues;
        [FieldOffset(3)]
        public fixed byte Reserved[3];
    }
}
