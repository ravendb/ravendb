using System;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Binary;

namespace Voron.Util;

public sealed unsafe class BloomFilter
{
    private const ulong K = 4;

    private readonly byte[] _bits;

    public BloomFilter(int capacity)
    {
        _bits = new byte[capacity];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Add(long value)
    {
        ulong numberOfBits = (ulong)_bits.Length * BitVector.BitsPerByte;
        ulong hash = Hashing.XXHash64.CalculateInline((byte*)&value, sizeof(long), seed: 1);
        ulong step = Hashing.XXHash64.CalculateInline((byte*)&value, sizeof(long), seed: 1337);

        bool isNew = false;
        for (ulong i = 0; i < K; i++)
        {
            // Dillinger and Manolios enhanced double hashing via forward differencing
            ulong finalHash = hash % numberOfBits;
            ref byte ptr = ref _bits[finalHash / BitVector.BitsPerByte];
            int bit = 1 << (int)(finalHash % BitVector.BitsPerByte);
            isNew |= (ptr & bit) == 0;
            ptr |= (byte)bit;

            hash += step;
            step += i;
        }

        return isNew;
    }

    public void Clear() => Array.Clear(_bits);
}
