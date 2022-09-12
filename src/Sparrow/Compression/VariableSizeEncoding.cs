using System;
using System.Runtime.CompilerServices;

namespace Sparrow.Compression
{
    public static class VariableSizeEncoding
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadMany<T>(ReadOnlySpan<byte> buffer, int count, Span<T> output) where T : unmanaged
        {
            if (output.Length < count)
                goto FailCount;

            int pos = 0;
            
            for (int i = 0; i < count; i++)
            {
                if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
                {
                    if (typeof(T) == typeof(bool))
                        output[i] = (T)(object)(buffer[pos + 0] == 1);
                    else if (typeof(T) == typeof(sbyte))
                        output[i] = (T)(object)(sbyte)buffer[pos + 0];
                    else
                        output[i] = (T)(object)buffer[pos + 0];

                    pos += 1;
                    continue;
                }

                int offset = 0;

                if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong) ||
                    typeof(T) == typeof(int) || typeof(T) == typeof(uint) ||
                    typeof(T) == typeof(short) || typeof(T) == typeof(ushort))
                {
                    long ul = 0;

                    byte b = buffer[pos + 0];
                    ul |= (long)(b & 0x7F);
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = buffer[pos + 1];
                    ul |= (long)(b & 0x7F) << 7;
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = buffer[pos + 2];
                    ul |= (long)(b & 0x7F) << 14;
                    offset++;

                    // This is of type size 2 bytes, therefore we will cut it short here. 
                    if (typeof(T) == typeof(short) || typeof(T) == typeof(ushort))
                    {
                        if ((b & 0x80) != 0)
                            goto Fail;

                        // PERF: This unconditional jump force the JIT to remove all the dead code. 
                        goto End;
                    }

                    if ((b & 0x80) == 0)
                        goto End;

                    b = buffer[pos + 3];
                    ul |= (long)(b & 0x7F) << 21;
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = buffer[pos + 4];
                    ul |= (long)(b & 0x7F) << 28;
                    offset++;

                    // This is of type size 4 bytes, therefore we will cut it short here.  
                    if (typeof(T) == typeof(int) || typeof(T) == typeof(uint))
                    {
                        if ((b & 0x80) != 0)
                            goto Fail;

                        // PERF: This unconditional jump force the JIT to remove all the dead code. 
                        goto End;
                    }

                    if ((b & 0x80) == 0)
                        goto End;

                    b = buffer[pos + 5];
                    ul |= (long)(b & 0x7F) << 35;
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = buffer[pos + 6];
                    ul |= (long)(b & 0x7F) << 42;
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = buffer[pos + 7];
                    ul |= (long)(b & 0x7F) << 49;
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = buffer[pos + 8];
                    ul |= (long)(b & 0x7F) << 56;
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = buffer[pos + 9];
                    ul |= (long)(b & 0x7F) << 63;
                    offset++;
                    if ((b & 0x80) != 0)
                        goto Fail;

                    End:
                    pos += offset;

                    if (typeof(T) == typeof(short))
                        output[i] = (T)(object)(short)ul;
                    else if (typeof(T) == typeof(ushort))
                        output[i] = (T)(object)(ushort)ul;
                    else if (typeof(T) == typeof(int))
                        output[i] = (T)(object)(int)ul;
                    else if (typeof(T) == typeof(uint))
                        output[i] = (T)(object)(uint)ul;
                    else if (typeof(T) == typeof(ulong))
                        output[i] = (T)(object)(ulong)ul;
                    else
                        output[i] = (T)(object)ul;

                    continue;
                }

                goto FailSupport;
            }

            return pos;

            FailSupport: ThrowNotSupportedException<T>();
            FailCount: ThrowNotEnoughOutputSpace();
            Fail: ThrowInvalidShift();
            return -1;
        }

        private static void ThrowNotEnoughOutputSpace()
        {
            throw new ArgumentException("Not enough output space.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining )]
        public static unsafe T Read<T>(byte* input, out int offset) where T : unmanaged
        {
            if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
            {
                offset = 1;
                byte b = input[0];

                if (typeof(T) == typeof(bool))
                    return (T)(object)(b == 1);
                return (typeof(T) == typeof(sbyte)) ? (T)(object)(sbyte)b : (T)(object)b;
            }

            offset = 0;

            if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong) ||
                typeof(T) == typeof(int) || typeof(T) == typeof(uint) ||
                typeof(T) == typeof(short) || typeof(T) == typeof(ushort))
            {
                const int bitsToShift = 7;

                var buffer = input;

                ulong ul = 0;
                byte b = buffer[0];
                offset++;
                if (b < 0x80)
                {
                    ul = (ulong)(b & 0x7F);
                    goto End;
                }

                byte b1 = buffer[1];
                ul = (b1 & 0x7Ful) << bitsToShift | (b & 0x7Ful);
                offset++;
                if (b1 < 0x80)
                    goto End;

                int bytesReadWhenOverflow;
                if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong))
                    bytesReadWhenOverflow = 10;
                else if (typeof(T) == typeof(int) || typeof(T) == typeof(uint))
                    bytesReadWhenOverflow = 5;
                else
                    bytesReadWhenOverflow = 3;

                // PERF: We need this for the JIT to understand that this can be profitable. 
                buffer += 2;
                int shift = 2 * bitsToShift;
                do
                {
                    if (shift == bytesReadWhenOverflow * bitsToShift)
                        goto Fail; // PERF: Using goto to diminish the size of the loop.

                    b = *buffer;
                    ul |= (b & 0x7Ful) << shift;
                    shift += bitsToShift;

                    offset++;
                    buffer++;
                }
                while (b >= 0x80);

                End:
                if (typeof(T) == typeof(short))
                    return (T)(object)(short)ul;
                if (typeof(T) == typeof(ushort))
                    return (T)(object)(ushort)ul;
                if (typeof(T) == typeof(int))
                    return (T)(object)(int)ul;
                if (typeof(T) == typeof(uint))
                    return (T)(object)(uint)ul;
                if (typeof(T) == typeof(long))
                    return (T)(object)(long)ul;
                return (T)(object)ul;
                
            }

            ThrowNotSupportedException<T>();

        Fail:
            ThrowInvalidShift();
            return (T)(object)-1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe T ReadCompact<T>(byte* input, out int offset, out bool success) where T : unmanaged
        {
            if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
            {
                offset = 1;
                byte b = input[0];
                success = true;

                if (typeof(T) == typeof(bool))
                    return (T)(object)(b == 1);
                return (T)(object)b;
            }

            offset = 0;

            if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong) ||
                typeof(T) == typeof(int) || typeof(T) == typeof(uint) ||
                typeof(T) == typeof(short) || typeof(T) == typeof(ushort))
            {
                const int bitsToShift = 7;
                int maxBytesWithoutOverflow;
                if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong))
                    maxBytesWithoutOverflow = 10;
                else if (typeof(T) == typeof(int) || typeof(T) == typeof(uint))
                    maxBytesWithoutOverflow = 5;
                else
                    maxBytesWithoutOverflow = 3;


                byte* buffer = input;

                ulong count = 0;
                byte b;
                byte shift = 0;
                do
                {
                    if (shift == maxBytesWithoutOverflow * bitsToShift)
                        goto Error; // PERF: Using goto to diminish the size of the loop.

                    b = *buffer;
                    buffer++;
                    offset++;

                    count |= (b & 0x7Ful) << shift;
                    shift += bitsToShift;
                }
                while (b >= 0x80);

                success = true;

                if (typeof(T) == typeof(short))
                    return (T)(object)(short)count;
                if (typeof(T) == typeof(ushort))
                    return (T)(object)(ushort)count;
                if (typeof(T) == typeof(int))
                    return (T)(object)(int)count;
                if (typeof(T) == typeof(uint))
                    return (T)(object)(uint)count;
                if (typeof(T) == typeof(long))
                    return (T)(object)(long)count;
                return (T)(object)count;

                Error:
                success = false;
                return (T)(object)-1;
            }

            throw new NotSupportedException("Unsupported type.");
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe T Read<T>(ReadOnlySpan<byte> input, out int offset, int pos = 0) where T : unmanaged
        {
            if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
            {
                offset = 1;
                byte b = input[pos + 0];

                if (typeof(T) == typeof(bool))
                    return (T)(object)(b == 1);
                return (T)(object)b;
            }

            fixed (byte* buffer = input)
            {
                return Read<T>(buffer + pos, out offset);
            }
        }

        private static void ThrowNotSupportedException<T>()
        {
            throw new NotSupportedException($"The type {nameof(T)} is not supported to be written.");
        }

        private static void ThrowInvalidShift()
        {
            throw new FormatException("Bad variable size int");
        }

        public static int GetMaximumEncodingLength(int count)
        {
            return 9 * count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining )]
        public static int WriteMany<T>(Span<byte> dest, Span<T> values, int pos = 0) where T : unmanaged
        {
            ReadOnlySpan<T> roValues = values;
            return WriteMany(dest, roValues, pos);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining )]
        public static int WriteMany<T>(Span<byte> dest, ReadOnlySpan<T> values, int pos = 0) where T : unmanaged
        {
            if ((dest.Length - pos) < (Unsafe.SizeOf<T>() + 1) * values.Length)
                ThrowNotEnoughOutputSpace();

            int startPos = pos;
            for (int i = 0; i < values.Length; i++)
                pos += Write(dest, values[i], pos);

            return pos - startPos;
        }

        public static unsafe int MaximumSizeOf<T>() where T: unmanaged
        {
            int i = (sizeof(T) * 8 / 7) +1;
            return i;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe int Write<T>(byte* dest, T value) where T : unmanaged
        {
            if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
            {
                byte b;
                if (typeof(T) == typeof(bool))
                    b = (bool)(object)value ? (byte)1 : (byte)0;
                else if (typeof(T) == typeof(sbyte))
                    b = (byte)(sbyte)(object)value;
                else
                    b = (byte)(object)value;

                int i = 0;
                if (b < 0x80)
                    goto End;
                dest[0] = (byte)(b | 0x80);
                b >>= 7;
                i++;

            End:
                dest[i] = b;
                return i + 1;
            }

            if (typeof(T) == typeof(short) || typeof(T) == typeof(ushort))
            {
                ushort ui;
                if (typeof(T) == typeof(short))
                    ui = (ushort)(short)(object)value;
                else
                    ui = (ushort)(object)value;

                if (ui < 0x80)
                {
                    *dest = (byte)ui;
                    return 1;
                }

                *dest = (byte)(ui | 0x80);
                ui >>= 7;
                dest++;

                int i = 1;
                while (ui >= 0x80)
                {
                    *dest++ = (byte)(ui | 0x80);
                    ui >>= 7;
                    i++;
                }

                *dest++ = (byte)ui;
                i++;

                return i;
            }

            if (typeof(T) == typeof(int) || typeof(T) == typeof(uint))
            {
                uint ui;
                if (typeof(T) == typeof(short))
                    ui = (uint)(short)(object)value;
                else if (typeof(T) == typeof(ushort))
                    ui = (ushort)(object)value;
                else if (typeof(T) == typeof(int))
                    ui = (uint)(int)(object)value;
                else
                    ui = (uint)(object)value;

                if (ui < 0x80)
                {
                    *dest = (byte)ui;
                    return 1;
                }

                *dest = (byte)(ui | 0x80);
                ui >>= 7;
                dest++;

                int i = 1;
                while (ui >= 0x80)
                {
                    *dest++ = (byte)(ui | 0x80);
                    ui >>= 7;
                    i++;
                }

                *dest++ = (byte)ui;
                i++;

                return i;
            }

            if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong))
            {
                ulong ul;
                if (typeof(T) == typeof(long))
                    ul = (ulong)(long)(object)value;
                else
                    ul = (ulong)(object)value;

                if (ul < 0x80)
                {
                    *dest = (byte)ul;
                    return 1;
                }
                
                *dest = (byte)(ul | 0x80);
                ul >>= 7;
                dest++;

                int i = 1;
                while (ul >= 0x80)
                {
                    *dest++ = (byte)(ul | 0x80);
                    ul >>= 7;
                    i++;
                }

                *dest++ = (byte)ul;
                i++;

                return i;
            }

            ThrowNotSupportedException<T>();
            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe int Write<T>(Span<byte> dest, T value, int pos = 0) where T : unmanaged
        {
            if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
            {
                byte b;
                if (typeof(T) == typeof(bool))
                    b = (bool)(object)value ? (byte)1 : (byte)0;
                else if (typeof(T) == typeof(sbyte))
                    b = (byte)(sbyte)(object)value;
                else
                    b = (byte)(object)value;

                int i = 0;
                if (b < 0x80)
                    goto End;
                dest[pos] = (byte)(b | 0x80);
                b >>= 7;
                i++;

            End:
                dest[pos + i] = b;
                return i + 1;
            }
            
            fixed (byte* buffer = dest)
            {
                return Write(buffer + pos, value);
            }
        }
    }
}
