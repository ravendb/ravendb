using System.Runtime.InteropServices;

namespace Raven.Server.Documents.TimeSeries
{
    [StructLayout(LayoutKind.Explicit, Size = 42)]
    public unsafe struct StatefulTimeStampValue
    {
        [FieldOffset(0)]
        public TimeStampValue First;
        [FieldOffset(8)]
        public TimeStampValue Last;
        [FieldOffset(16)]
        public TimeStampValue Max;
        [FieldOffset(24)]
        public TimeStampValue Min;
        [FieldOffset(32)]
        public TimeStampValue Sum;
        [FieldOffset(8)]
        public byte LeadingZeroes;
        [FieldOffset(9)]
        public byte TrailingZeroes;
    }

    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct TimeStampValue
    {
        [FieldOffset(0)]
        public double DoubleValue;
        [FieldOffset(0)]
        public long LongValue;
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
