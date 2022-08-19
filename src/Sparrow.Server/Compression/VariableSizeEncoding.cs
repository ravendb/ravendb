using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Platform.Posix;

namespace Sparrow.Server.Compression
{
    public static class VariableSizeEncoding
    {
        private const ulong ContinueMask = 0xFFFF_FFFF_FFFF_FF80;

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
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

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static unsafe T Read<T>(byte* input, out int offset) //where T : unmanaged
        {
            if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte) || typeof(T) == typeof(bool))
            {
                offset = 1;
                byte b = input[0];

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
                var buffer = input;

                long ul = 0;
                byte b = buffer[0];
                if (b < 0x80)
                {
                    ul = (long)(b & 0x7F);
                    offset = 1;
                    goto End;
                }

                byte b1 = buffer[1];
                ul = (long)(b1 & 0x7F) << bitsToShift | (long)(b & 0x7F);
                if (b1 < 0x80)
                {
                    offset = 2;
                    goto End;
                }

                // This is of type size 2 bytes, therefore we will cut it short here. 
                if (typeof(T) == typeof(short) || typeof(T) == typeof(ushort))
                {
                    b = buffer[2];
                    ul |= (long)(b & 0x7F) << 2 * bitsToShift;

                    if (b < 0x80)
                        goto Fail;

                    // PERF: This unconditional jump force the JIT to remove all the dead code.
                    offset = 3;
                    goto End;
                }

                bool ReadUnlikely(ref int offset)
                {
                    // PERF: We need this for the JIT to understand that this can be profitable. 
                    buffer += 2;
                    int shift = 2 * bitsToShift;
                    for (int i = 0; i < 9; i++)
                    {
                        b = buffer[i];
                        ul |= (long)(b & 0x7F) << shift;
                        shift += 7;

                        if (b < 0x80)
                        {
                            offset = 3 + i;
                            return true;
                        }
                    }

                    return false;
                }

                if (ReadUnlikely(ref offset))
                    goto End;

                if ((b & 0x7F) != 0)
                    goto Fail;

                End:

                if (typeof(T) == typeof(short))
                    return (T)(object)(short)ul;
                if (typeof(T) == typeof(ushort))
                    return (T)(object)(ushort)ul;
                if (typeof(T) == typeof(int))
                    return (T)(object)(int)ul;
                if (typeof(T) == typeof(uint))
                    return (T)(object)(uint)ul;
                if (typeof(T) == typeof(ulong))
                    return (T)(object)(ulong)ul;
                return (T)(object)ul;
                
            }

            ThrowNotSupportedException<T>();

        Fail:
            ThrowInvalidShift();
            return (T)(object)-1;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static unsafe T Read<T>(ReadOnlySpan<byte> input, out int offset, int pos = 0) //where T : unmanaged
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

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int WriteMany<T>(Span<byte> dest, Span<T> values, int pos = 0) where T : unmanaged
        {
            ReadOnlySpan<T> roValues = values;
            return WriteMany(dest, roValues, pos);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static int WriteMany<T>(Span<byte> dest, ReadOnlySpan<T> values, int pos = 0) where T : unmanaged
        {
            if ((dest.Length - pos) < (Unsafe.SizeOf<T>() + 1) * values.Length)
                ThrowNotEnoughOutputSpace();

            int startPos = pos;
            for (int i = 0; i < values.Length; i++)
                pos += Write(dest, values[i], pos);

            return pos - startPos;
        }

        private static ReadOnlySpan<byte> ContinuationMaskTable => new byte[] { 0x00, 0x80 };

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static unsafe int WriteInline<T>(byte* dest, T value) where T : unmanaged
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

            if (typeof(T) == typeof(short) || typeof(T) == typeof(ushort) || typeof(T) == typeof(int) || typeof(T) == typeof(uint))
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
                    dest[0] = (byte)ui;
                    return 1;
                }

                dest[0] = (byte)(ui | 0x80);

                uint b2 = ui >> 7;
                uint b3 = ui >> 14;
                dest[1] = (byte)(b2 | ContinuationMaskTable[(b2 >= 0x80).ToInt32()]);
                if (b2 < 0x80)
                    return 2;

                uint b4 = ui >> 21;
                uint b5 = ui >> 28;
                dest[2] = (byte)(b3 | ContinuationMaskTable[(b3 >= 0x80).ToInt32()]);
                if (b3 < 0x80)
                    return 3;

                dest[3] = (byte)(b4 | ContinuationMaskTable[(b4 >= 0x80).ToInt32()]);
                if (b4 < 0x80)
                    return 4;

                dest[4] = (byte)b5;
                return 5;
            }

            if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong))
            {
                ulong ul;
                if (typeof(T) == typeof(long))
                    ul = (ulong)(long)(object)value;
                else
                    ul = (ulong)(object)value;

                ulong b2 = ul >> 7;
                if (ul < 0x80)
                {
                    dest[0] = (byte)ul;
                    return 1;
                }

                dest[0] = (byte)(ul | 0x80);
                dest[1] = (byte)(b2 | ContinuationMaskTable[(b2 >= 0x80).ToInt32()]);
                if (b2 < 0x80)
                    return 2;

                ulong b3 = ul >> 14;
                ulong b4 = ul >> 21;
                dest[2] = (byte)(b3 | ContinuationMaskTable[(b3 >= 0x80).ToInt32()]);
                if (b3 < 0x80)
                    return 3;

                ulong b5 = ul >> 28;
                dest[3] = (byte)(b4 | ContinuationMaskTable[(b4 >= 0x80).ToInt32()]);
                if (b4 < 0x80)
                    return 4;

                dest[4] = (byte)(b5 | ContinuationMaskTable[(b5 >= 0x80).ToInt32()]);
                if (b5 < 0x80)
                    return 5;

                ul >>= 35;
                dest[5] = (byte)(ul | 0x80);
                ul >>= 7;
                int i = 6;

                if (ul < 0x80)
                    goto End;
                dest[6] = (byte)(ul | 0x80);
                ul >>= 7;
                i++;

                if (ul < 0x80)
                    goto End;
                dest[7] = (byte)(ul | 0x80);
                ul >>= 7;
                i++;

                if (ul < 0x80)
                    goto End;
                dest[8] = (byte)(ul | 0x80);
                ul >>= 7;
                i++;

                End:
                dest[i] = (byte)ul;
                return i + 1;
            }

            ThrowNotSupportedException<T>();
            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
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

            if (typeof(T) == typeof(short) || typeof(T) == typeof(ushort) || typeof(T) == typeof(int) || typeof(T) == typeof(uint))
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
                    dest[0] = (byte)ui;
                    return 1;
                }

                dest[0] = (byte)(ui | 0x80);
                return WriteIntUnlikely(ui);

                [MethodImpl(MethodImplOptions.NoInlining)]
                int WriteIntUnlikely(uint u)
                {
                    uint b2 = u >> 7;
                    uint b3 = u >> 14;
                    dest[1] = (byte)(b2 | ContinuationMaskTable[(b2 >= 0x80).ToInt32()]);
                    if (b2 < 0x80)
                        return 2;

                    uint b4 = u >> 21;
                    uint b5 = u >> 28;
                    dest[2] = (byte)(b3 | ContinuationMaskTable[(b3 >= 0x80).ToInt32()]);
                    if (b3 < 0x80)
                        return 3;

                    dest[3] = (byte)(b4 | ContinuationMaskTable[(b4 >= 0x80).ToInt32()]);
                    if (b4 < 0x80)
                        return 4;

                    dest[4] = (byte)b5;
                    return 5;
                }
            }

            if (typeof(T) == typeof(long) || typeof(T) == typeof(ulong))
            {
                ulong ul;
                if (typeof(T) == typeof(long))
                    ul = (ulong)(long)(object)value;
                else
                    ul = (ulong)(object)value;

                ulong b2 = ul >> 7;
                if (ul < 0x80)
                {
                    dest[0] = (byte)ul;
                    return 1;
                }

                dest[0] = (byte)(ul | 0x80);
                dest[1] = (byte)(b2 | ContinuationMaskTable[(b2 >= 0x80).ToInt32()]);
                if (b2 < 0x80)
                    return 2;

                return WriteLongUnlikely(ul);

                [MethodImpl(MethodImplOptions.NoInlining)]
                int WriteLongUnlikely(ulong u)
                {
                    ulong b3 = u >> 14;
                    ulong b4 = u >> 21;
                    dest[2] = (byte)(b3 | ContinuationMaskTable[(b3 >= 0x80).ToInt32()]);
                    if (b3 < 0x80)
                        return 3;

                    ulong b5 = u >> 28;
                    dest[3] = (byte)(b4 | ContinuationMaskTable[(b4 >= 0x80).ToInt32()]);
                    if (b4 < 0x80)
                        return 4;

                    dest[4] = (byte)(b5 | ContinuationMaskTable[(b5 >= 0x80).ToInt32()]);
                    if (b5 < 0x80)
                        return 5;

                    ul = u >> 35;
                    dest[5] = (byte)(ul | 0x80);
                    ul >>= 7;
                    int i = 6;

                    if (ul < 0x80)
                        goto End;
                    dest[6] = (byte)(ul | 0x80);
                    ul >>= 7;
                    i++;

                    if (ul < 0x80)
                        goto End;
                    dest[7] = (byte)(ul | 0x80);
                    ul >>= 7;
                    i++;

                    if (ul < 0x80)
                        goto End;
                    dest[8] = (byte)(ul | 0x80);
                    ul >>= 7;
                    i++;

                End:
                    dest[i] = (byte)ul;
                    return i + 1;
                }
            }

            ThrowNotSupportedException<T>();
            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static unsafe int Write<T>(Span<byte> dest, T value, int pos = 0, bool forceInline = false) where T : unmanaged
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
                if (forceInline)
                    return WriteInline(buffer + pos, value);
                
                return Write(buffer + pos, value);
            }
        }
    }
}
