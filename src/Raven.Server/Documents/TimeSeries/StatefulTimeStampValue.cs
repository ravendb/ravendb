using System;
using System.Runtime.InteropServices;

namespace Raven.Server.Documents.TimeSeries
{
    [StructLayout(LayoutKind.Explicit, Size = 48)]
    public unsafe struct StatefulTimestampValue
    {
        [FieldOffset(0)]
        public double First;
        [FieldOffset(8)]
        public double Last;
        [FieldOffset(8)]
        public long PreviousValue; // same as Last

        [FieldOffset(16)]
        public double Max;
        [FieldOffset(24)]
        public double Min;
        [FieldOffset(32)]
        public double Sum;
        [FieldOffset(40)]
        public int Count;
        [FieldOffset(44)]
        public byte LeadingZeroes;
        [FieldOffset(45)]
        public byte TrailingZeroes;
        [FieldOffset(46)]
        public fixed byte Reserved[2];
    }

    public unsafe struct StatefulTimestampValueSpan
    {
        private StatefulTimestampValue* Pointer;
        private int Length;

        public StatefulTimestampValueSpan(StatefulTimestampValue* pointer, int length)
        {
            Pointer = pointer;
            Length = length;
        }

        public Span<StatefulTimestampValue> Span => new Span<StatefulTimestampValue>(Pointer, Length);
    }

    public struct TimestampState
    {
        public byte LeadingZeroes;
        public byte TrailingZeroes;
    }
}
