using System;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Collections;
using Sparrow.Utils;
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

        //RavenDB-25738 For backward compatibility only. Use only for document IDs, collection, attachment name & type, and timeseries tag
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FindEscapedPositionsAndEscapeControlsForBackwardCompatibility(byte* str, ref int len, int previousComputedMaxSize)
        {
            StringUtils.FindEscapedPositionsAndEscapeControlsForBackwardCompatibility(EscapePositions, str, ref len, previousComputedMaxSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FindEscapedPositions(byte* str, int len, int previousComputedMaxSize) => StringUtils.FindEscapedPositions(EscapePositions, str, len, previousComputedMaxSize);

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

            ClearBuffered();
        }

        internal FastList<(JsonParserToken, long)> _bufferedSequence;
        private decimal _maxLong;
        private decimal _minLong;
        private double _maxDoubleMagnitude;

        internal bool IsBufferedFloat { get; private set; }

        internal void ClearBuffered()
        {
            _bufferedSequence?.WeakClear();

            // We will change this value to MinValue because the 0 magnitude number is valid as the content of a vector.
            _maxLong = long.MinValue;
            _minLong = long.MaxValue;
            _maxDoubleMagnitude = double.MinValue;
            IsBufferedFloat = false;
        }

        internal BlittableVectorType GetBufferedOptimalType()
        {
            if (IsBufferedFloat)
            {
                // We need to check both because we can convert longs to doubles, but not the other
                // way around. 
                var maxValue = Math.Max(decimal.ToDouble(Math.Max(Math.Abs(_maxLong), Math.Abs(_minLong))), _maxDoubleMagnitude);

                // If handling floating-point values, use a simple check first.
                return Math.Abs(maxValue) switch
                {
                    <= float.MaxValue => BlittableVectorType.Float,
                    _ => BlittableVectorType.Double,
                };
            }

            // At this point we know we are only working with integers.
            // Also, we're prioritizing signed types over unsigned for parsing purposes
            return (minLong: _minLong, maxLong: _maxLong) switch
            {
                (>= sbyte.MinValue, <= sbyte.MaxValue) => BlittableVectorType.SByte,
                (>= byte.MinValue, <= byte.MaxValue) => BlittableVectorType.Byte,

                (>= short.MinValue, <= short.MaxValue) => BlittableVectorType.Int16,
                (>= ushort.MinValue, <= ushort.MaxValue) => BlittableVectorType.UInt16,

                (>= int.MinValue, <= int.MaxValue) => BlittableVectorType.Int32,
                (>= uint.MinValue, <= uint.MaxValue) => BlittableVectorType.UInt32,
                
                (>= long.MinValue, <= long.MaxValue) => BlittableVectorType.Int64,
                (>= ulong.MinValue, _) => BlittableVectorType.UInt64, // Fallback for unsigned long
                (_, _) => throw new InvalidDataException("Unable to determine a type for values ranging between {_minLong} and {_maxLong}.")
            };
        }

        internal void AddBuffered(long stateLong)
        {
            _bufferedSequence ??= new();

            _maxLong = Math.Max(stateLong, _maxLong);
            _minLong = Math.Min(stateLong, _minLong);

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
