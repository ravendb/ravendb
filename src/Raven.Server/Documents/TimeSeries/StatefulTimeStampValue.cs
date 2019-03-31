using System.Runtime.InteropServices;

namespace Raven.Server.Documents.TimeSeries
{
    [StructLayout(LayoutKind.Explicit, Size = 10)]
    public unsafe struct StatefulTimeStampValue
    {
        [FieldOffset(0)]
        public double DoubleValue;
        [FieldOffset(0)]
        public long LongValue;
        
        [FieldOffset(8)]
        public byte LeadingZeroes;
        [FieldOffset(9)]
        public byte TrailingZeroes;
    }

    [StructLayout(LayoutKind.Explicit, Size = 10)]
    public struct TimeStampState
    {
        [FieldOffset(0)]
        public byte LeadingZeroes;
        [FieldOffset(1)]
        public byte TrailingZeroes;
    }
}
