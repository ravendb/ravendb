using System;
using System.Runtime.InteropServices;

namespace Raven.Server.Documents.TimeSeries
{
    [StructLayout(LayoutKind.Explicit, Size = 48)]
    public unsafe struct StatefulTimestampValue
    {
        [FieldOffset(0)]
        public long RawFirst;
        public double First
        {
            get => BitConverter.Int64BitsToDouble(RawFirst);
            set => RawFirst = BitConverter.DoubleToInt64Bits(value);
        }

        public double Last
        {
            get => BitConverter.Int64BitsToDouble(PreviousValue);
        }

        [FieldOffset(8)]
        public long PreviousValue; // same as Last

        [FieldOffset(16)]
        public long RawMax;
        public double Max
        {
            get => BitConverter.Int64BitsToDouble(RawMax);
            set => RawMax = BitConverter.DoubleToInt64Bits(value);
        }

        [FieldOffset(24)]
        public long RawMin;
        public double Min
        {
            get => BitConverter.Int64BitsToDouble(RawMin);
            set => RawMin = BitConverter.DoubleToInt64Bits(value);
        }

        [FieldOffset(32)]
        public long RawSum;
        public double Sum
        {
            get => BitConverter.Int64BitsToDouble(RawSum);
            set => RawSum = BitConverter.DoubleToInt64Bits(value);
        }

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

        public int NumberOfEntries => Span.Length > 0 ? Span[0].Count : 0;
    }

    public struct TimestampState
    {
        public byte LeadingZeroes;
        public byte TrailingZeroes;
        public long LastValidValue;
    }
}
