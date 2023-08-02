using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Sparrow.Compression
{
    internal static class VariableSizeEncoding
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadMany<T>(ReadOnlySpan<byte> buffer, int count, Span<T> output) where T : unmanaged
        {
            if (output.Length < count)
                goto FailCount;


            ref var bufferRef = ref MemoryMarshal.GetReference(buffer);

            int pos = 0;
            for (int i = 0; i < count; i++)
            {
                ref var outputRef = ref Unsafe.Add(ref MemoryMarshal.GetReference(output), i);
                if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
                {
                    
                    if (typeof(T) == typeof(bool))
                        outputRef = (T)(object)(Unsafe.Add(ref bufferRef, pos) == 1);
                    else if (typeof(T) == typeof(sbyte))
                        outputRef = (T)(object)(sbyte)Unsafe.Add(ref bufferRef, pos);
                    else
                        outputRef = (T)(object)Unsafe.Add(ref bufferRef, pos);

                    pos += 1;
                    continue;
                }

                int offset = 0;

                if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong) ||
                    typeof(T) == typeof(int) || typeof(T) == typeof(uint) ||
                    typeof(T) == typeof(short) || typeof(T) == typeof(ushort))
                {
                    long ul = 0;

                    byte b = Unsafe.Add(ref bufferRef, pos);
                    ul |= (long)(b & 0x7F);
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = Unsafe.Add(ref bufferRef, pos + 1);
                    ul |= (long)(b & 0x7F) << 7;
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = Unsafe.Add(ref bufferRef, pos + 2);
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

                    b = Unsafe.Add(ref bufferRef, pos + 3);
                    ul |= (long)(b & 0x7F) << 21;
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = Unsafe.Add(ref bufferRef, pos + 4);
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

                    b = Unsafe.Add(ref bufferRef, pos + 5);
                    ul |= (long)(b & 0x7F) << 35;
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = Unsafe.Add(ref bufferRef, pos + 6);
                    ul |= (long)(b & 0x7F) << 42;
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = Unsafe.Add(ref bufferRef, pos + 7);
                    ul |= (long)(b & 0x7F) << 49;
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = Unsafe.Add(ref bufferRef, pos + 8);
                    ul |= (long)(b & 0x7F) << 56;
                    offset++;
                    if ((b & 0x80) == 0)
                        goto End;

                    b = Unsafe.Add(ref bufferRef, pos + 9);
                    ul |= (long)(b & 0x7F) << 63;
                    offset++;
                    if ((b & 0x80) != 0)
                        goto Fail;

                    End:
                    pos += offset;

                    if (typeof(T) == typeof(short))
                        outputRef = (T)(object)(short)ul;
                    else if (typeof(T) == typeof(ushort))
                        outputRef = (T)(object)(ushort)ul;
                    else if (typeof(T) == typeof(int))
                        outputRef = (T)(object)(int)ul;
                    else if (typeof(T) == typeof(uint))
                        outputRef = (T)(object)(uint)ul;
                    else if (typeof(T) == typeof(ulong))
                        outputRef = (T)(object)(ulong)ul;
                    else
                        outputRef = (T)(object)ul;

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe T Read<T>(byte* input, out int offset, out bool success) where T : unmanaged
        {
            if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
            {
                offset = 1;
                byte b = input[0];
                success = true;

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
                success = true;
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
            success = false;

            if (typeof(T) == typeof(short))
                return (T)(object)(short)0;
            if (typeof(T) == typeof(ushort))
                return (T)(object)(ushort)0;
            if (typeof(T) == typeof(int))
                return (T)(object)(int)0;
            if (typeof(T) == typeof(uint))
                return (T)(object)(uint)0;
            if (typeof(T) == typeof(long))
                return (T)(object)(long)0;
            return (T)(object)(ulong)0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T Read<T>(ReadOnlySpan<byte> input, int pos, out int offset, out bool success) where T : unmanaged
        {
            success = true;

            ref var inputStart = ref Unsafe.Add(ref MemoryMarshal.GetReference(input), pos);
            if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
            {
                offset = 1;

                if (typeof(T) == typeof(bool))
                    return (T)(object)(inputStart == 1);
                return (typeof(T) == typeof(sbyte)) ? (T)(object)(sbyte)inputStart : (T)(object)inputStart;
            }

            if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong) ||
                typeof(T) == typeof(int) || typeof(T) == typeof(uint) ||
                typeof(T) == typeof(short) || typeof(T) == typeof(ushort))
            {
                const int bitsToShift = 7;

                ulong ul;
                byte b = inputStart;
                if (b < 0x80)
                {
                    ul = (ulong)(b & 0x7F);
                    offset = 1;
                    goto End;
                }

                byte b1 = Unsafe.Add(ref inputStart, 1);
                ul = (b1 & 0x7Ful) << bitsToShift | (b & 0x7Ful);
                if (b1 < 0x80)
                {
                    offset = 2;
                    goto End;
                }

                int bytesReadWhenOverflow;
                if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong))
                    bytesReadWhenOverflow = 10;
                else if (typeof(T) == typeof(int) || typeof(T) == typeof(uint))
                    bytesReadWhenOverflow = 5;
                else
                    bytesReadWhenOverflow = 3;

                // PERF: We need this for the JIT to understand that this can be profitable. 
                ref var inputRef = ref Unsafe.Add(ref inputStart, 2);
                int shift = 2 * bitsToShift;
                do
                {
                    if (shift == bytesReadWhenOverflow * bitsToShift)
                        goto Fail; // PERF: Using goto to diminish the size of the loop.

                    b = inputRef;
                    ul |= (b & 0x7Ful) << shift;
                    shift += bitsToShift;

                    inputRef = Unsafe.Add(ref inputRef, 1);
                }
                while (b >= 0x80);
                offset = Unsafe.ByteOffset(ref inputRef, ref inputStart).ToInt32();

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

            Fail:
            success = false;
            offset = 0;

            if (typeof(T) == typeof(short))
                return (T)(object)(short)0;
            if (typeof(T) == typeof(ushort))
                return (T)(object)(ushort)0;
            if (typeof(T) == typeof(int))
                return (T)(object)(int)0;
            if (typeof(T) == typeof(uint))
                return (T)(object)(uint)0;
            if (typeof(T) == typeof(long))
                return (T)(object)(long)0;
            return (T)(object)(ulong)0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe T Read<T>(byte* input, out int offset) where T : unmanaged
        {
            T result = Read<T>(input, out offset, out var success);
            if (success)
                return result;

            ThrowInvalidShift();
            return (T)(object)-1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe byte* Skip<T>(byte* input) where T : unmanaged
        {
            if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
            {
                return input + 1;
            }

            int offset = 0;

            if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong) ||
                typeof(T) == typeof(int) || typeof(T) == typeof(uint) ||
                typeof(T) == typeof(short) || typeof(T) == typeof(ushort))
            {
                int bytesReadWhenOverflow;
                if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong))
                    bytesReadWhenOverflow = 10;
                else if (typeof(T) == typeof(int) || typeof(T) == typeof(uint))
                    bytesReadWhenOverflow = 5;
                else
                    bytesReadWhenOverflow = 3;

                // PERF: We need this for the JIT to understand that this can be profitable. 
                var buffer = input;

                byte b;
                do
                {
                    if (offset == bytesReadWhenOverflow)
                        goto Fail; // PERF: Using goto to diminish the size of the loop.

                    b = *(buffer + offset);
                    offset++;
                }
                while (b >= 0x80);

                return input + offset;
            }

            ThrowNotSupportedException<T>();

        Fail:
            return input;
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
        public static T ReadCompact<T>(ReadOnlySpan<byte> input, int pos, out int offset, out bool success) where T : unmanaged
        {
            ref var inputRef = ref Unsafe.Add(ref MemoryMarshal.GetReference(input), pos);
            if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
            {
                offset = 1;
                success = true;

                if (typeof(T) == typeof(bool))
                    return (T)(object)(inputRef == 1);
                return (T)(object)inputRef;
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

                ulong count = 0;
                byte b;
                byte shift = 0;
                do
                {
                    if (shift == maxBytesWithoutOverflow * bitsToShift)
                        goto Error; // PERF: Using goto to diminish the size of the loop.

                    b = inputRef;
                    inputRef = ref Unsafe.Add(ref inputRef, 1);
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
            }

            Error:
            success = false;
            return (T)(object)-1;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T Read<T>(ReadOnlySpan<byte> input, out int offset, int pos = 0) where T : unmanaged
        {
            ref var destRef = ref Unsafe.Add(ref MemoryMarshal.GetReference(input), pos);
            if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
            {
                offset = 1;

                if (typeof(T) == typeof(bool))
                    return (T)(object)(destRef == 1);
                return (T)(object)destRef;
            }

            if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
            {
                offset = 1;
                if (typeof(T) == typeof(bool))
                    return (T)(object)(destRef == 1);
                return (typeof(T) == typeof(sbyte)) ? (T)(object)(sbyte)destRef : (T)(object)destRef;
            }

            offset = 0;

            if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong) ||
                typeof(T) == typeof(int) || typeof(T) == typeof(uint) ||
                typeof(T) == typeof(short) || typeof(T) == typeof(ushort))
            {
                const int bitsToShift = 7;

                ulong ul = 0;
                byte b = destRef;
                offset++;
                if (b < 0x80)
                {
                    ul = (ulong)(b & 0x7F);
                    goto End;
                }

                byte b1 = Unsafe.Add(ref destRef, 1);
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
                destRef = ref Unsafe.Add(ref destRef, 2);
                int shift = 2 * bitsToShift;
                do
                {
                    if (shift == bytesReadWhenOverflow * bitsToShift)
                        goto Fail; // PERF: Using goto to diminish the size of the loop.

                    b = destRef;
                    ul |= (b & 0x7Ful) << shift;
                    shift += bitsToShift;

                    offset++;
                    destRef = ref Unsafe.Add(ref destRef, 1);
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

        private static void ThrowNotSupportedException<T>()
        {
            throw new NotSupportedException($"The type {nameof(T)} is not supported to be written.");
        }

        private static void ThrowInvalidShift()
        {
            throw new FormatException("Bad variable size int");
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
            {
                ref var valueToWrite = ref Unsafe.Add(ref MemoryMarshal.GetReference(values), i);
                pos += Write(dest, valueToWrite, pos);
            }

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
        public static int Write<T>(Span<byte> dest, T value, int pos = 0) where T : unmanaged
        {
            ref var destRef = ref Unsafe.Add(ref MemoryMarshal.GetReference(dest), pos);

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

                destRef = (byte)(b | 0x80);
                b >>= 7;
                i++;

            End:
                Unsafe.Add(ref destRef, i) = b;
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
                    destRef = (byte)ui;
                    return 1;
                }

                destRef = (byte)(ui | 0x80);
                ui >>= 7;

                destRef = ref Unsafe.Add(ref destRef, 1);

                int i = 1;
                while (ui >= 0x80)
                {
                    destRef = (byte)(ui | 0x80);
                    destRef = ref Unsafe.Add(ref destRef, 1);
                    ui >>= 7;
                    i++;
                }

                destRef = (byte)ui;
                destRef = ref Unsafe.Add(ref destRef, 1);
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
                    destRef = (byte)ui;
                    return 1;
                }

                destRef = (byte)(ui | 0x80);
                ui >>= 7;
                destRef = ref Unsafe.Add(ref destRef, 1);

                int i = 1;
                while (ui >= 0x80)
                {
                    destRef = (byte)(ui | 0x80);
                    destRef = ref Unsafe.Add(ref destRef, 1);
                    ui >>= 7;
                    i++;
                }

                destRef = (byte)ui;
                destRef = ref Unsafe.Add(ref destRef, 1);
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
                    destRef = (byte)ul;
                    return 1;
                }

                destRef = (byte)(ul | 0x80);
                destRef = ref Unsafe.Add(ref destRef, 1);
                
                ul >>= 7;
                int i = 1;
                while (ul >= 0x80)
                {
                    destRef = (byte)(ul | 0x80);
                    destRef = ref Unsafe.Add(ref destRef, 1);
                    ul >>= 7;
                    i++;
                }

                destRef = (byte)ul;
                destRef = ref Unsafe.Add(ref destRef, 1);
                i++;

                return i;
            }

            ThrowNotSupportedException<T>();
            return -1;
        }
    }
}
