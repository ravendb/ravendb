using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Collections;
using InvalidOperationException = System.InvalidOperationException;

namespace Sparrow.Json.Parsing
{
    public sealed unsafe class JsonParserState
    {
        public const int EscapePositionItemSize = 5;
        public const int ControlCharacterItemSize = 5;
        public byte* StringBuffer;
        public int StringSize;
        public int? CompressedSize;
        public long Long;
        public JsonParserToken CurrentTokenType;
        public JsonParserTokenContinuation Continuation;

        public readonly FastList<int> EscapePositions = new();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteVariableSizeInt(ref byte* dest, int value)
        {
            // assume that we don't use negative values very often
            var v = (uint)value;
            while (v >= 0x80)
            {
                *dest++ = (byte)(v | 0x80);
                v >>= 7;
            }
            *dest++ = (byte)(v);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int VariableSizeIntSize(int value)
        {
            int count = 0;
            // assume that we don't use negative values very often
            var v = (uint)value;
            while (v >= 0x80)
            {
                v >>= 7;
                count++;
            }
            count++;
            return count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FindEscapePositionsMaxSize(string str, out int escapedCount)
        {
            return FindEscapePositionsMaxSize(str.AsSpan(), out escapedCount);
        }
        
        public static int FindEscapePositionsMaxSize(ReadOnlySpan<char> str, out int escapedCount)
        {
            var count = 0;
            var controlCount = 0;

            for (int i = 0; i < str.Length; i++)
            {
                var value = str[i];
                // PERF: We use the values directly because it is 5x faster than iterating over a constant array.
                // 8  => '\b' => 0000 1000
                // 9  => '\t' => 0000 1001
                // 10 => '\n' => 0000 1010

                // 12 => '\f' => 0000 1100
                // 13 => '\r' => 0000 1101

                // 34 => '"'  => 0010 0010
                // 92 => '\\' => 0101 1100

                if (value == 92 || value == 34 || (value >= 8 && value <= 13 && value != 11))
                {
                    count++;
                    continue;
                }

                if (value < 32)
                {
                    controlCount++;
                }
            }

            escapedCount = controlCount;
            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * EscapePositionItemSize + controlCount * ControlCharacterItemSize;
        }

        public static int FindEscapePositionsMaxSize(byte* str, int size, out int escapedCount)
        {
            var count = 0;
            var controlCount = 0;

            for (int i = 0; i < size; i++)
            {
                byte value = str[i];

                // PERF: We use the values directly because it is 5x faster than iterating over a constant array.
                // 8  => '\b' => 0000 1000
                // 9  => '\t' => 0000 1001
                // 10 => '\n' => 0000 1010

                // 12 => '\f' => 0000 1100
                // 13 => '\r' => 0000 1101

                // 34 => '"'  => 0010 0010
                // 92 => '\\' => 0101 1100

                if (value == 92 || value == 34 || (value >= 8 && value <= 13 && value != 11))
                {
                    count++;
                    continue;
                }

                if (value < 32)
                {
                    controlCount++;
                }
            }

            escapedCount = controlCount;
            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * EscapePositionItemSize + controlCount * ControlCharacterItemSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FindEscapePositionsIn(byte* str, ref int len, int previousComputedMaxSize)
        {
            FindEscapePositionsIn(EscapePositions, str, ref len, previousComputedMaxSize);
        }

        public static void FindEscapePositionsIn(FastList<int> buffer, byte* str, ref int len, int previousComputedMaxSize)
        {
            var originalLen = len;
            buffer.Clear();
            if (previousComputedMaxSize == EscapePositionItemSize)
            {
                // if the value is 5, then we got no escape positions, see: FindEscapePositionsMaxSize
                // and we don't have to do any work
                return;
            }

            var lastEscape = 0;
            for (int i = 0; i < len; i++)
            {
                byte value = str[i];

                // PERF: We use the values directly because it is 5x faster than iterating over a constant array.
                // 8  => '\b' => 0000 1000
                // 9  => '\t' => 0000 1001
                // 13 => '\r' => 0000 1101
                // 10 => '\n' => 0000 1010
                // 12 => '\f' => 0000 1100
                // 34 => '"'  => 0010 0010
                // 92 => '\\' => 0101 1100

                if (value == 92 || value == 34 || (value is >= 8 and <= 13 && value != 11))
                {
                    buffer.Add(i - lastEscape);
                    lastEscape = i + 1;
                    continue;
                }
                //Control character ascii values
                if (value < 32)
                {
                    if (len + ControlCharacterItemSize > originalLen + previousComputedMaxSize)
                        ThrowInvalidSizeForEscapeControlChars(previousComputedMaxSize);

                    // move rest of buffer 
                    // write \u0000
                    // update size
                    var from = str + i + 1;
                    var to = str + i + 1 + ControlCharacterItemSize;
                    var sizeToCopy = len - i - 1;
                    //here we only shifting by 5 bytes since we are going to override the byte at the current position.
                    // source and destination blocks may overlap so we using Buffer.MemoryCopy to handle that scenario.
                    Buffer.MemoryCopy(from, to, (uint)sizeToCopy, (uint)sizeToCopy);
                    str[i] = (byte)'\\';
                    str[i + 1] = (byte)'u';
                    *(int*)(str + i + 2) = AbstractBlittableJsonTextWriter.ControlCodeEscapes[value];
                    //The original string already had one byte so we only added 5.
                    len += ControlCharacterItemSize;
                    i += ControlCharacterItemSize;
                }
            }
        }
        private static void ThrowInvalidSizeForEscapeControlChars(int previousComputedMaxSize)
        {
            throw new InvalidOperationException($"The previousComputedMaxSize: {previousComputedMaxSize} is too small to support the required escape positions. Did you not call FindMaxNumberOfEscapePositions?");
        }
        public int WriteEscapePositionsTo(byte* buffer)
        {
            var escapePositions = EscapePositions;
            var originalBuffer = buffer;
            WriteVariableSizeInt(ref buffer, escapePositions.Count);

            // PERF: Using a for in this way will evict the bounds-check and also avoid the cost of using an struct enumerator. 
            for (int i = 0; i < escapePositions.Count; i++)
                WriteVariableSizeInt(ref buffer, escapePositions[i]);

            return (int)(buffer - originalBuffer);
        }

        public void Reset()
        {
            StringBuffer = null;
            StringSize = 0;
            CompressedSize = null;
            Long = 0;
            CurrentTokenType = JsonParserToken.None;
            Continuation = JsonParserTokenContinuation.None;
            EscapePositions.Clear();

            // We will change this value to MinValue because the 0 magnitude number is valid as the content of a vector.
            _maxLongMagnitude = long.MinValue;
            _maxDoubleMagnitude = double.MinValue;
        }

        internal FastList<(JsonParserToken, long)> _bufferedSequence;
        private long _maxLongMagnitude;
        private double _maxDoubleMagnitude;
        private bool _hasNegativeValue;

        internal bool IsBufferedFloat { get; private set; }

        internal void ClearBuffered()
        {
            _bufferedSequence.WeakClear();

            // We will change this value to MinValue because the 0 magnitude number is valid as the content of a vector.
            _maxLongMagnitude = long.MinValue;
            _maxDoubleMagnitude = double.MinValue;
        }

        internal BlittableVectorType GetBufferedOptimalType()
        {
            if (IsBufferedFloat)
            {
                // We need to check both because we can convert longs to doubles, but not the other
                // way around. 
                double maxValue = Math.Max(_maxDoubleMagnitude, _maxLongMagnitude);
                
                // If handling floating-point values, use a simple check first.
                return Math.Abs(maxValue) switch
                {
                    <= float.MaxValue => BlittableVectorType.Float,
                    _ => BlittableVectorType.Double,
                };
            }

            // At this point we know we are only working with integers.
            return (_maxBufferedLongMagnitude: _maxLongMagnitude, _hasNegativeLongValue: _hasNegativeValue) switch
            {
                ( <= sbyte.MaxValue, true) => BlittableVectorType.SByte,
                ( <= byte.MaxValue, false) => BlittableVectorType.Byte,
                ( <= short.MaxValue, true) => BlittableVectorType.Int16,
                ( <= ushort.MaxValue, false) => BlittableVectorType.UInt16,
                ( <= int.MaxValue, true) => BlittableVectorType.Int32,
                ( <= uint.MaxValue, false) => BlittableVectorType.UInt32,
                (_, true) => BlittableVectorType.Int64,
                (_, false) => BlittableVectorType.UInt64, // Fallback for unsigned long
            };
        }

        internal void AddBuffered(long stateLong)
        {
            _bufferedSequence ??= new();

            _maxLongMagnitude = Math.Max(Math.Abs(stateLong), _maxLongMagnitude);
            _hasNegativeValue |= stateLong < 0;

            _bufferedSequence.Add((JsonParserToken.Integer, stateLong));
        }

        internal void AddBuffered(double stateFloat)
        {
            _bufferedSequence ??= new();

            // Since if floating point we don't really care if it has negative values, we ignore it.
            // On the other handle we do care to find the maximum magnitude of the floating point
            // in order to know if we can use a float instead of a double.
            IsBufferedFloat = true;

            // We only care about the magnitude.
            _maxDoubleMagnitude = Math.Max(Math.Abs(stateFloat), _maxDoubleMagnitude);

            // We don't want to do a proper casting, what we need is to reinterpret the bitstream here. 
            _bufferedSequence.Add((JsonParserToken.Float, Unsafe.As<double, long>(ref stateFloat)));
        }

        internal int FillVector<T>(Span<T> vector) where T : unmanaged
        {
            PortableExceptions.ThrowIfOnDebug<ArgumentException>(vector.Length < _bufferedSequence.Count);

            var sequence = _bufferedSequence;
            for (int i = 0; i < vector.Length; i++)
            {
                ref var item = ref sequence.GetAsRef(i);

                if (item.Item1 == JsonParserToken.Float)
                {
                    // Since we have stored a double in the value we will reinterpret it back into a double,
                    // and then we are going to perform the casting to the proper type we are required to 
                    // return.
                    double valueAsFloatingPoint = Unsafe.As<long, double>(ref item.Item2);
                    if (typeof(T) == typeof(double))
                    {
                        vector[i] = (T)(object)valueAsFloatingPoint;
                        continue;
                    }

                    if (typeof(T) == typeof(float))
                    {
                        vector[i] = (T)(object)(float)valueAsFloatingPoint;
                        continue;
                    }
                    
#if NET6_0_OR_GREATER
                    if (typeof(T) == typeof(Half))
                    {
                        vector[i] = (T)(object)(Half)valueAsFloatingPoint;
                        continue;
                    }
#endif
                    // This shouldn't happen. If it does, there is a defect in the implementation.
                    PortableExceptions.Throw<InvalidOperationException>($"The type {typeof(T).Name} is not supported when we have buffered floating point values.");
                }
                else
                {
                    // Since we have stored a long value we will reinterpret it back into the proper type we are required to 
                    // return. We know this is an integer value.
                    long valueAsLong = item.Item2;

                    if (typeof(T) == typeof(double))
                    {
                        vector[i] = (T)(object)(double)valueAsLong;
                        continue;
                    }

                    if (typeof(T) == typeof(float))
                    {
                        vector[i] = (T)(object)(float)valueAsLong;
                        continue;
                    }
#if NET6_0_OR_GREATER
                    if (typeof(T) == typeof(Half))
                    {
                        vector[i] = (T)(object)(Half)valueAsLong;
                        continue;
                    }
#endif
                    if (typeof(T) == typeof(long))
                    {
                        vector[i] = (T)(object)valueAsLong;
                        continue;
                    }

                    if (typeof(T) == typeof(ulong))
                    {
                        vector[i] = (T)(object)(ulong)valueAsLong;
                        continue;
                    }

                    if (typeof(T) == typeof(int))
                    {
                        vector[i] = (T)(object)(int)valueAsLong;
                        continue;
                    }

                    if (typeof(T) == typeof(uint))
                    {
                        vector[i] = (T)(object)(uint)valueAsLong;
                        continue;
                    }

                    if (typeof(T) == typeof(short))
                    {
                        vector[i] = (T)(object)(short)valueAsLong;
                        continue;
                    }

                    if (typeof(T) == typeof(ushort))
                    {
                        vector[i] = (T)(object)(ushort)valueAsLong;
                        continue;
                    }

                    if (typeof(T) == typeof(sbyte))
                    {
                        vector[i] = (T)(object)(sbyte)valueAsLong;
                        continue;
                    }

                    if (typeof(T) == typeof(byte))
                    {
                        vector[i] = (T)(object)(byte)valueAsLong;
                        continue;
                    }

                    // This shouldn't happen. If it does, there is a defect in the implementation.
                    PortableExceptions.Throw<InvalidOperationException>($"The type {typeof(T).Name} is not supported when we have buffered floating point values.");
                }
            }

            return sequence.Count;
        }
    }
}
