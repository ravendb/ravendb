using System;
using System.Runtime.InteropServices;

namespace Raven.Server.Documents.TimeSeries
{
    [StructLayout(LayoutKind.Explicit, Size = 48)]
    public unsafe struct StatefulTimeStampValue
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

    public unsafe struct StatefulTimeStampValueSpan
    {
        private StatefulTimeStampValue* Pointer;
        private int Length;


        public StatefulTimeStampValueSpan(StatefulTimeStampValue* pointer, int length)
        {
            Pointer = pointer;
            Length = length;
        }

        public Span<StatefulTimeStampValue> Span => new Span<StatefulTimeStampValue>(Pointer, Length);
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
