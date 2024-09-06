using Sparrow;
using System;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Runtime.Intrinsics;
using Sparrow.Server;
using Sparrow.Server.Utils.VxSort;
using Voron.Data.Fixed;

namespace Voron.Impl.FreeSpace;

public unsafe struct StreamBitArray
{
    private const int CountOfItems = 64;

    private fixed uint _inner[CountOfItems];
    public int SetCount;

    public StreamBitArray()
    {
        SetCount = 0;
        Vector256<uint>.Zero.StoreUnsafe(ref _inner[0]);
        Vector256<uint>.Zero.StoreUnsafe(ref _inner[8]);
        Vector256<uint>.Zero.StoreUnsafe(ref _inner[16]);
        Vector256<uint>.Zero.StoreUnsafe(ref _inner[24]);
        Vector256<uint>.Zero.StoreUnsafe(ref _inner[32]);
        Vector256<uint>.Zero.StoreUnsafe(ref _inner[40]);
        Vector256<uint>.Zero.StoreUnsafe(ref _inner[48]);
        Vector256<uint>.Zero.StoreUnsafe(ref _inner[56]);
    }

    public StreamBitArray(byte* ptr)
    {
        var ints = (uint*)ptr;
        SetCount = (int)*ints;
        var a = Vector256.LoadUnsafe(ref ints[1]);
        var b = Vector256.LoadUnsafe(ref ints[9]);
        var c = Vector256.LoadUnsafe(ref ints[17]);
        var d = Vector256.LoadUnsafe(ref ints[25]);
        var e = Vector256.LoadUnsafe(ref ints[33]);
        var f = Vector256.LoadUnsafe(ref ints[41]);
        var g = Vector256.LoadUnsafe(ref ints[49]);
        var h = Vector256.LoadUnsafe(ref ints[57]);

        a.StoreUnsafe(ref _inner[0]);
        b.StoreUnsafe(ref _inner[8]);
        c.StoreUnsafe(ref _inner[16]);
        d.StoreUnsafe(ref _inner[24]);
        e.StoreUnsafe(ref _inner[32]);
        f.StoreUnsafe(ref _inner[40]);
        g.StoreUnsafe(ref _inner[48]);
        h.StoreUnsafe(ref _inner[56]);
    }

    public unsafe void Write(FixedSizeTree freeSpaceTree, long sectionId)
    {
        using (freeSpaceTree.DirectAdd(sectionId, out _, out var ptr))
        {
            Write(ptr);
        }
    }

    private void Write(byte* ptr)
    {
        var ints = (uint*)ptr;
        *ints = (uint)SetCount;
        var a = Vector256.LoadUnsafe(ref _inner[0]);
        var b = Vector256.LoadUnsafe(ref _inner[8]);
        var c = Vector256.LoadUnsafe(ref _inner[16]);
        var d = Vector256.LoadUnsafe(ref _inner[24]);
        var e = Vector256.LoadUnsafe(ref _inner[32]);
        var f = Vector256.LoadUnsafe(ref _inner[40]);
        var g = Vector256.LoadUnsafe(ref _inner[48]);
        var h = Vector256.LoadUnsafe(ref _inner[56]);

        a.StoreUnsafe(ref ints[1]);
        b.StoreUnsafe(ref ints[9]);
        c.StoreUnsafe(ref ints[17]);
        d.StoreUnsafe(ref ints[25]);
        e.StoreUnsafe(ref ints[33]);
        f.StoreUnsafe(ref ints[41]);
        g.StoreUnsafe(ref ints[49]);
        h.StoreUnsafe(ref ints[57]);
    }

    public int FirstSetBit(int bitsToStart)
    {
        int vectorStart = (bitsToStart / 256) * Vector256<int>.Count;
        var scalarSearch = bitsToStart % 256;
        if (scalarSearch != 0)
        {
            if (TryScalarSearch(scalarSearch, vectorStart, out int trailingZeroCount)) 
                return trailingZeroCount;

            vectorStart += Vector256<int>.Count;
        }
        for (int i = vectorStart; i < CountOfItems; i += Vector256<int>.Count)
        {
            var a = Vector256.LoadUnsafe(ref _inner[i]);
            var gt = Vector256.GreaterThan(a, Vector256<uint>.Zero);
            if (gt == Vector256<uint>.Zero)
            {
                continue;
            }
            var mask = gt.ExtractMostSignificantBits();
            var idx = BitOperations.TrailingZeroCount(mask) + i;
            var item = _inner[idx];
            return idx * 32 + BitOperations.TrailingZeroCount(item);
        }
        return -1;
    }

    private bool TryScalarSearch(int scalarSearch, int vectorStart, out int trailingZeroCount)
    {
        for (int i = scalarSearch / 32; i < Vector256<int>.Count; i++)
        {
            var bitsToZero = scalarSearch % 32;
            scalarSearch = 0;
            var bits = _inner[vectorStart + i] & (-1 << bitsToZero);
            if (bits != 0)
            {
                {
                    trailingZeroCount = (vectorStart+i) * 32 + BitOperations.TrailingZeroCount(bits);
                    return true;
                }
            }
        }

        trailingZeroCount = -1;
        return false;
    }

    public bool Get(int index)
    {
        return (_inner[index >> 5] & (1 << (index & 31))) != 0;
    }

    public void Set(int index, bool value)
    {
        if (value)
        {
            _inner[index >> 5] |= (uint)(1 << (index & 31));
            SetCount++;
        }
        else
        {
            _inner[index >> 5] &= (uint)~(1 << (index & 31));
            SetCount--;
        }
    }

    public int GetEndRangeCount()
    {
        int count = 0;
        for (int i = CountOfItems - Vector256<uint>.Count; i >= 0; i -= Vector256<uint>.Count)
        {
            var a = Vector256.LoadUnsafe(ref _inner[i]);
            if (a == Vector256<uint>.AllBitsSet)
            {
                count += 256;
                continue;
            }

            for (int j = i + Vector256<uint>.Count - 1; j >= 0; j--)
            {
                if (_inner[j] == uint.MaxValue)
                {
                    count += 32;
                    continue;
                }

                count += BitOperations.LeadingZeroCount(~_inner[j]);
                break;
            }

            break;
        }

        return count;
    }

    public bool HasStartRangeCount(int max)
    {
        int count = 0;
        for (int i = 0; i < CountOfItems; i += Vector256<int>.Count)
        {
            var a = Vector256.LoadUnsafe(ref _inner[i]);
            var eq = Vector256.Equals(a, Vector256<uint>.AllBitsSet);
            if (eq == Vector256<uint>.AllBitsSet)
            {
                count += 256;
                if (count >= max)
                    return true;
                
                continue;
            }
            for (int j =  i; j < i + Vector256<uint>.Count; j--)
            {
                if (_inner[j] == uint.MaxValue)
                {
                    count += 32;
                    if (count >= max)
                        return true;

                    continue;
                }

                count += BitOperations.TrailingZeroCount(~_inner[j]);
                break;
            }
            break;
        }

        return count >= max;
    }
}
