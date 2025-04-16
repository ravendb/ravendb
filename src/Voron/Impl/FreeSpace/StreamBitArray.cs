using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow.Json.Parsing;
using Voron.Data.Fixed;

namespace Voron.Impl.FreeSpace;

public unsafe struct StreamBitArray
{
    private const int CountOfWords = 64;
    private const int BitsInWord = 32;
    private const int TotalBits = CountOfWords * BitsInWord;

    private fixed uint _inner[CountOfWords];
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

    public int? GetContinuousRangeStart(int num)
    {
        return num switch
        {
            1 => FirstSetBit(),
            < BitsInWord => FindSmallRange(num),
            < 64 => FindLargeRange<int>(num),
            _ => FindLargeRange<long>(num)
        };
    }

    private int? FirstSetBit()
    {
        for (int i = 0; i < CountOfWords; i += Vector256<uint>.Count)
        {
            var a = Vector256.LoadUnsafe(ref _inner[i]);
            var gt = Vector256.GreaterThan(a, Vector256<uint>.Zero);
            if (gt == Vector256<uint>.Zero)
                continue;

            var mask = gt.ExtractMostSignificantBits();
            var idx = BitOperations.TrailingZeroCount(mask) + i;
            var item = _inner[idx];
            return idx * 32 + BitOperations.TrailingZeroCount(item);
        }

        return null;
    }

    private int? FindSmallRange(int num)
    {
        // finding sequences up to 32 bits
        for (var i = 0; i < CountOfWords; i++)
        {
            uint current = _inner[i];
            if (current == 0)
                continue;

            if (current == uint.MaxValue)
                return i * BitsInWord;

            var currentCopy = current;
            int numCopy = num - 1;

            // find consecutive range: https://stackoverflow.com/a/37903049/6366
            // perform AND operations with shifted versions of the number
            // this will leave 1s only where there were n consecutive 1s
            while (currentCopy != 0 && numCopy-- > 0)
            {
                currentCopy &= (currentCopy << 1);
            }

            if (currentCopy != 0)
            {
                int position = BitOperations.TrailingZeroCount(currentCopy);
                return i * BitsInWord + position - (num - 1);
            }

            if (i == CountOfWords - 1)
            {
                // this is the last word, no next word to check with
                break;
            }

            // we didn't find the sequence in the word, let's check it between words
            int numberOfSetBitsCurrent = BitOperations.LeadingZeroCount((uint)~current);
            var nextWord = _inner[i + 1];
            var numberOfSetBitsNext = BitOperations.TrailingZeroCount(~nextWord);

            if (numberOfSetBitsCurrent + numberOfSetBitsNext >= num)
                return (i * BitsInWord) + (BitsInWord - numberOfSetBitsCurrent);
        }

        return null;
    }

    private int? FindLargeRange<T>(int num)
        where T : unmanaged, INumber<T>
    {
        // finding sequences larger than 32 bits
        // the idea is that we look for sequences that bridge across words using leading/trailing zero counts
        var start = -1;
        var count = 0;

        int currentBitsInWord = sizeof(T) * 8;
        int spanLength = CountOfWords * sizeof(uint) / sizeof(T);

        fixed (void* p = &_inner[0])
        {
            var span = new Span<T>(p, spanLength);

            for (var i = 0; i < span.Length; i++)
            {
                T current = span[i];
                if (current == T.Zero)
                {
                    start = -1;
                    count = 0;
                    continue;
                }

                if (current == -T.One)
                {
                    if (start == -1)
                    {
                        start = i * currentBitsInWord;
                    }

                    count += currentBitsInWord;
                    if (count >= num)
                        return start;

                    continue;
                }

                if (start == -1)
                {
                    // find trailing ones at the end of the word if no sequence has started
                    CheckTrailingSequence();
                }
                else
                {
                    if (count + (TotalBits - i * currentBitsInWord) < num)
                    {
                        // impossible to satisfy the continuous bit requirement
                        return null;
                    }

                    if (count + (currentBitsInWord - 1) < num)
                    {
                        // impossible to satisfy the continuous bit requirement in this word
                        CheckTrailingSequence();
                        continue;
                    }

                    // we look at the beginning of the word
                    int numberOfSetBits = current switch
                    {
                        int integer => BitOperations.TrailingZeroCount(~integer),
                        long l => BitOperations.TrailingZeroCount(~l),
                        _ => throw new NotSupportedException()
                    };
                    count += numberOfSetBits;
                    if (count >= num)
                        return start;

                    // reset for the next sequence
                    CheckTrailingSequence();
                }

                void CheckTrailingSequence()
                {
                    int numberOfSetBits = current switch
                    {
                        int integer => BitOperations.LeadingZeroCount(~(uint)integer),
                        long l => BitOperations.LeadingZeroCount(~(ulong)l),
                        _ => throw new NotSupportedException()
                    };
                    if (numberOfSetBits == 0)
                    {
                        start = -1;
                        count = 0;
                    }
                    else
                    {
                        // Calculate the starting bit position in the array
                        start = (i * currentBitsInWord) + (currentBitsInWord - numberOfSetBits);
                        count = numberOfSetBits;
                    }
                }
            }

        }

        return null;
    }

    public int NextUnsetBits(int start)
    {
        return FirstSetBit<Inverse>(start);
    }

    public int FirstSetBit(int bitsToStart)
    {
        return FirstSetBit<Nothing>(bitsToStart);
    }

    private int FirstSetBit<TModify>(int bitsToStart)
        where TModify : struct, IModifyBuffer
    {
        int vectorStart = (bitsToStart / 256) * Vector256<int>.Count;
        var scalarSearch = bitsToStart % 256;
        if (scalarSearch != 0)
        {
            if (TryScalarSearch<TModify>(scalarSearch, vectorStart, out int trailingZeroCount))
                return trailingZeroCount;

            vectorStart += Vector256<int>.Count;
        }
        for (int i = vectorStart; i < CountOfWords; i += Vector256<int>.Count)
        {
            var a = default(TModify).Modify(Vector256.LoadUnsafe(ref _inner[i]));
            var gt = Vector256.GreaterThan(a, Vector256<uint>.Zero);
            if (gt == Vector256<uint>.Zero)
            {
                continue;
            }
            var mask = gt.ExtractMostSignificantBits();
            var idx = BitOperations.TrailingZeroCount(mask) + i;
            var item = default(TModify).Modify(_inner[idx]);
            return idx * 32 + BitOperations.TrailingZeroCount(item);
        }
        return -1;
    }

    private bool TryScalarSearch<TModify>(int scalarSearch, int vectorStart, out int trailingZeroCount)
        where TModify : struct, IModifyBuffer
    {
        for (int i = scalarSearch / 32; i < Vector256<int>.Count; i++)
        {
            var bitsToZero = scalarSearch % 32;
            scalarSearch = 0;
            var bits = default(TModify).Modify(_inner[vectorStart + i]) & (-1 << bitsToZero);
            if (bits != 0)
            {
                trailingZeroCount = (vectorStart + i) * 32 + BitOperations.TrailingZeroCount(bits);
                return true;
            }
        }

        trailingZeroCount = -1;
        return false;
    }

    private struct Nothing : IModifyBuffer
    {
        public uint Modify(uint i) => i;

        public Vector256<uint> Modify(Vector256<uint> v) => v;
    }

    private struct Inverse : IModifyBuffer
    {
        public uint Modify(uint i) => ~i;

        public Vector256<uint> Modify(Vector256<uint> v) => ~v;
    }

    private interface IModifyBuffer
    {
        uint Modify(uint i);
        Vector256<uint> Modify(Vector256<uint> v);
    }

    public bool Get(int index)
    {
        return (_inner[index >> 5] & (1 << (index & 31))) != 0;
    }

    public void Set(int index, bool value)
    {
        var current = Get(index);
        if (current == value)
            ThrowValueAlreadyExists(index, value);

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

    [DoesNotReturn]
    private static void ThrowValueAlreadyExists(int index, bool value)
    {
        throw new InvalidOperationException($"the index {index} is already has the requested value: {value}");
    }

    public int GetEndRangeCount()
    {
        var count = 0;
        fixed (uint* ptr = _inner)
        {
            long* span = (long*)ptr;
            const int length = CountOfWords / 2;

            for (var i = length - 1; i >= 0; i--)
            {
                long current = span[i];
                if (current == 0)
                    break;

                if (current == -1)
                {
                    count += BitsInWord * 2;
                    continue;
                }

                int numberOfSetBits = BitOperations.LeadingZeroCount(~(ulong)current);
                count += numberOfSetBits;

                // we won't find any more continuous bits after this.
                break;
            }
        }
        return count;
    }

    public int GetStartRangeCount()
    {
        var count = 0;

        fixed (uint* ptr = _inner)
        {
            long* span = (long*)ptr;

            for (var i = 0; i < CountOfWords / 2; i++)
            {
                long current = span[i];

                if (current == 0)
                    break;

                if (current == -1)
                {
                    count += BitsInWord * 2;
                    continue;
                }

                int numberOfSetBits = BitOperations.TrailingZeroCount(~current);
                count += numberOfSetBits;

                // we won't find any more continuous bits after this.
                return count;
            }
        }

        return count;
    }

    public bool HasStartRangeCount(int max)
    {
        var count = 0;

        fixed (uint* ptr = _inner)
        {
            long* span = (long*)ptr;

            for (var i = 0; i < CountOfWords / 2; i++)
            {
                long current = span[i];

                if (current == 0)
                    break;

                if (current == -1)
                {
                    count += BitsInWord * 2;
                    if (count >= max)
                        return true;
                    continue;
                }

                int numberOfSetBits = BitOperations.TrailingZeroCount(~current);
                count += numberOfSetBits;

                // we won't find any more continuous bits after this.
                return count >= max;
            }
        }

        return false;
    }

    public DynamicJsonValue ToJson(long key, bool hex)
    {
        object[] collection = new object[CountOfWords];
        for (var i = 0; i < CountOfWords; i++)
            collection[i] = hex ? _inner[i].ToString("X") : _inner[i];

        return new DynamicJsonValue
        {
            ["Key"] = key,
            [nameof(SetCount)] = SetCount,
            ["Data"] = new DynamicJsonArray(collection)
        };
    }


    public override string ToString()
    {
        uint[] array = new uint[CountOfWords];
        for (int i = 0; i < CountOfWords; i++)
        {
            array[i] = _inner[i];
        }
        return string.Join(", ", array);
    }
}
