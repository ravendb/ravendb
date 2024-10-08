using Sparrow;
using System;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Sparrow.Server;
using Sparrow.Server.Utils.VxSort;
using Sparrow.Threading;
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

    public void Write(FixedSizeTree freeSpaceTree, long sectionId)
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

    public int FirstSetBit()
    {
        for (int i = 0; i < CountOfItems; i += Vector256<uint>.Count)
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

    public int FindRange(int num)
    {
        return num switch
        {
            >= 32 => FindRangeUsingWords(num),
            _ => FindRangeUsingBits(num)
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool FindRangeInVector(Vector256<uint> v, int num, out int bits)
    {
        // find consecutive range: https://stackoverflow.com/a/37903049/6366
        // note that this is only _inside_ words
        for (int j = 0; j < num-1; j++)
        {
            v &= v << 1;
        }
            
        var gt = Vector256.GreaterThan(v, Vector256<uint>.Zero);
        if (gt == Vector256<uint>.Zero)
        {
            bits = -1;
            return false;
        }
            
        var mask = gt.ExtractMostSignificantBits();
        int vectorIndex = BitOperations.TrailingZeroCount(mask);
        uint element = v.GetElement(vectorIndex);
        var item = element - (element & (element - 1));
        var bitPos = BitOperations.TrailingZeroCount(item) - (num - 1);
        bits = bitPos + vectorIndex * 32;
        return true;
    }

    private int FindRangeUsingBits(int num)
    {
        ref uint shiftedInner = ref Unsafe.AddByteOffset(ref _inner[0], 2);
        // we are skipping the first 2 bytes, and we are ***reading 2 bytes past the buffer***
        // this is fine to do, since we know that after the buffer, we have the SetCount field
        Debug.Assert(
            Unsafe.AsPointer(ref SetCount) == Unsafe.AsPointer(ref _inner[CountOfItems]),
            "We are intentionally reading past the end of the buffer"
        );
        for (int i = 0; i < CountOfItems; i += Vector256<uint>.Count)
        {
            var aligned = Vector256.LoadUnsafe(ref _inner[0], (nuint)i);
            var unaligned = Vector256.LoadUnsafe(ref shiftedInner, (nuint)i);

            bool hasAligned = FindRangeInVector(aligned, num, out int alignedBits);
            bool hasUnaligned = FindRangeInVector(unaligned, num, out int unalignedBits);

            var alignedPos = i * 32 + alignedBits;
            var unalignedPos = i * 32 + unalignedBits + 16;
            if (unalignedPos + num > CountOfItems * 32)
                hasUnaligned = false;

            switch (hasAligned, hasUnaligned)
            {
                case (true, true):
                    return Math.Min(alignedPos, unalignedPos);
                case (true, false):
                    return alignedPos;
                case (false, true):
                    return unalignedPos;
            }
        }
        
        return -1;
    }

    private int FindRangeUsingWords(int num)
    {
        Debug.Assert(num >= 32, "num >= 32");
        int requiredWords = num / 32;
        int totalWords = requiredWords + (num % 32 > 0 ? 1 : 0);
            
        for (int i = 0; i < CountOfItems; i += Vector256<uint>.Count)
        {
            var a = Vector256.LoadUnsafe(ref _inner[i]);
            var eq = Vector256.Equals(a, Vector256<uint>.AllBitsSet);
            if (eq == Vector256<uint>.Zero)
                continue;
            
            var mask = eq.ExtractMostSignificantBits();
            int idx = BitOperations.TrailingZeroCount(mask) + i;
            
            if (idx + totalWords > CountOfItems)
                break;

            while(true)
            {
                Debug.Assert(_inner[idx] == uint.MaxValue, "_inner[idx] == uint.MaxValue - because it must be given the SIMD check");
                int remainingBits = num % 32;
                int found = idx * 32;
                if (idx > 0)
                {
                    var prev = _inner[idx - 1];
                    int previousBits = BitOperations.LeadingZeroCount(~prev);
                    if (previousBits > 0)
                    {
                        int bitsToTake = Math.Min(remainingBits, previousBits);
                        remainingBits -= bitsToTake;
                        found -= bitsToTake;
                    }
                }
                
                for (int j = 1; j < requiredWords; j++)
                {
                    if (_inner[idx + j] == uint.MaxValue) 
                        continue;

                    idx += j;
                    goto NextElement;
                }

                if (remainingBits == 0)
                    return found;

                var availableBits = BitOperations.TrailingZeroCount(~_inner[idx + requiredWords]);
                if(availableBits >= remainingBits)
                    return found;

                idx += requiredWords;
                    
                NextElement:
                while (idx < i + Vector256<uint>.Count && 
                       _inner[idx] != uint.MaxValue)
                {
                    idx++;
                }

                if (idx >= i + Vector256<uint>.Count)
                    break;
            }
        }

        return -1;
    }

    public bool Get(int index)
    {
        return (_inner[index >> 5] & (1 << (index & 31))) != 0;
    }

    public void Set(int index, int count, bool value)
    {
        for (int i = 0; i < count; i++)
        {
            Set(i + index, value);
        }
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
        Debug.Assert(max <= 2048, "max <= 2048 - maximum range inside the bit array");
        
        if (SetCount < max)
            return false; // not possible

        if (SetCount == CountOfItems)
            return true; // naturally true
        
        for (int i = 0; i < CountOfItems; i += Vector256<int>.Count)
        {
            var a = Vector256.LoadUnsafe(ref _inner[i]);
            var lt = Vector256.LessThan(a, Vector256<uint>.AllBitsSet);
            if (lt == Vector256<uint>.Zero)
            {
                continue;
            }
            var mask = lt.ExtractMostSignificantBits();
            var idx = BitOperations.TrailingZeroCount(mask) + i;
            var item = _inner[idx];
            var firstUnsetBit =  idx * 32 + BitOperations.TrailingZeroCount(~item);
            return firstUnsetBit >= max;
        }

        return true; // we should never reach here, mind
    }
}
