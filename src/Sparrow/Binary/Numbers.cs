using System;
using System.Runtime.CompilerServices;

namespace Sparrow.Binary;

public static class Numbers
{
    /// <summary>
    /// Hex string lookup table.
    /// </summary>
    private static ReadOnlySpan<byte> HexStringTable => new byte[]
    {
        (byte)'0', (byte)'1', (byte)'2', (byte)'3',
        (byte)'4', (byte)'5', (byte)'6', (byte)'7',
        (byte)'8', (byte)'9', (byte)'A', (byte)'B',
        (byte)'C', (byte)'D', (byte)'E', (byte)'F', 
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (byte, byte) ToHexChars(byte value)
    {
        return (HexStringTable[value >> 4], HexStringTable[value & 0b00001111]);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe int FillAsHex<T>(Span<byte> dest, T number) where T : unmanaged
    {
        if (typeof(T) == typeof(byte) || (typeof(T) == typeof(sbyte)))
        {
            byte b = typeof(T) == typeof(byte) ? (byte)(object)number : (byte)(sbyte)(object)number;
            (dest[0], dest[1]) = ToHexChars(b);
            return 2;
        }
        
        if (typeof(T) == typeof(short) || (typeof(T) == typeof(ushort)))
        {
            ushort* buffer = stackalloc ushort[1];
            buffer[0] = typeof(T) == typeof(ushort) ? (ushort)(object)number : (ushort)(short)(object)number;

            byte* asBytes = (byte*)buffer;
            (dest[0], dest[1]) = ToHexChars(asBytes[0]);
            (dest[2], dest[3]) = ToHexChars(asBytes[1]);
            return 4;
        }

        if ((typeof(T) == typeof(uint)) || (typeof(T) == typeof(int)))
        {
            uint* buffer = stackalloc uint[1];
            buffer[0] = typeof(T) == typeof(uint) ? (uint)(object)number : (uint)(int)(object)number;

            byte* asBytes = (byte*)buffer;
            (dest[0], dest[1]) = ToHexChars(asBytes[0]);
            (dest[2], dest[3]) = ToHexChars(asBytes[1]);
            (dest[4], dest[5]) = ToHexChars(asBytes[2]);
            (dest[6], dest[7]) = ToHexChars(asBytes[3]);
            return 8;
        }

        if ((typeof(T) == typeof(ulong)) || (typeof(T) == typeof(long)))
        {
            ulong* buffer = stackalloc ulong[1];
            buffer[0] = typeof(T) == typeof(ulong) ? (ulong)(object)number : (ulong)(long)(object)number;

            byte* asBytes = (byte*)buffer;
            (dest[0], dest[1]) = ToHexChars(asBytes[0]);
            (dest[2], dest[3]) = ToHexChars(asBytes[1]);
            (dest[4], dest[5]) = ToHexChars(asBytes[2]);
            (dest[6], dest[7]) = ToHexChars(asBytes[3]);
            (dest[8], dest[9]) = ToHexChars(asBytes[4]);
            (dest[10], dest[11]) = ToHexChars(asBytes[5]);
            (dest[12], dest[13]) = ToHexChars(asBytes[6]);
            (dest[14], dest[15]) = ToHexChars(asBytes[7]);

            return 16;
        }

        throw new NotSupportedException($"Type {typeof(T).Name} is not supported");
    }
}


