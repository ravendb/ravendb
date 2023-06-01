using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow.Server;

namespace Voron.Util.PFor;

public unsafe class FastPForEncoder  : IDisposable
{
    private readonly ByteStringContext _allocator;
    internal const int PrefixSizeBits = 10;

    private long* _entries;
    private byte* _entriesOutput;
    private long _entriesOutputCount = 0;
    private int _count;
    private int _offset;
    private ushort _sharedPrefix;
    private List<uint>[] _exceptions = new List<uint>[32];
    private int[] _exceptionsStart = new int[32];
    private List<byte> _metadata = new();
    private int _metadataPos;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _entriesOutputScope;


    public FastPForEncoder(ByteStringContext allocator)
    {
        _allocator = allocator;
    }
    
    public void Init(long* entries, int count)
    {
        _entries = entries;
        _count = count;
        _offset = 0;
        for (int i = 0; i < _exceptions.Length; i++)
        {
            _exceptions[i]?.Clear();
            _exceptionsStart[i] = 0;
        }
        _metadata.Clear();
        _metadataPos = 0;

        if (_entriesOutputCount < count)
        {
            var newCount = Math.Max(256, BitOperations.RoundUpToPowerOf2((uint)count));
            _entriesOutputScope.Dispose();
            _entriesOutputScope = _allocator.Allocate((int)newCount * sizeof(long), out ByteString bs);
            _entriesOutput = bs.Ptr;
            _entriesOutputCount = newCount;
        }
    }

    public int Encode()
    {
        (_sharedPrefix, var prefixShift) = RemoveSharedPrefix();

        var totalSize = sizeof(PForHeader);
        int i = 0;
        var entriesAsInt = (uint*)_entriesOutput;
        var entries = (long*)_entriesOutput;
        var prev = Vector256.Create(*entries);
        var max = Vector256.Create<long>(uint.MaxValue);
        for (; i + 256 <= _count; i += 256)
        {
            var blockStart = entriesAsInt;
            int j = 0;
            for (; j < 256; j += Vector256<long>.Count)
            {
                var cur = Vector256.Load(entries + i + j);
                var mixed = Vector256.Shuffle(cur, Vector256.Create(0, 0, 1, 2)) & Vector256.Create(0, -1, -1, -1) |
                            Vector256.Shuffle(prev, Vector256.Create(3, 3, 3, 3)) & Vector256.Create(-1, 0, 0, 0);
                prev = cur;
                var delta = cur - mixed;

                if (Vector256.GreaterThanAny(delta, max))
                    goto UnlikelyGreaterThanUintMax; // should be *rare*

                var deltaInts = Vector256.Narrow(delta.AsUInt64(), Vector256<ulong>.Zero);
                deltaInts.Store(entriesAsInt);
                // we write 8 values, but increment by 4, so we'll overwrite it next op
                entriesAsInt += Vector256<long>.Count;
            }
            totalSize += ProcessBlock(blockStart);
            continue;

            UnlikelyGreaterThanUintMax:
            UnlikelyEncodeBatchUsingVarint();
        }
        if (i < _count)
        {
            var remainder = _count - i;
            _metadata.Add((byte)255); // indication on varint encoded batch
            _metadata.Add((byte)remainder); // remainder batch size
            var prevIdx = Math.Max(0, i - 1);
            totalSize += ComputeVarintDeltaSize(entries[prevIdx], entries + i, remainder, prefixShift);
        }

        for (int j = 2; j < _exceptions.Length; j++)
        {
            var item = _exceptions[j];
            if (item == null || item.Count == 0)
                continue;
            totalSize += BitPacking.RequireSizeSegmented(item.Count, j);
        }

        totalSize += _metadata.Count;
        _metadataPos = 0;
        return totalSize;

        void UnlikelyEncodeBatchUsingVarint()
        {
            // this is not efficient, but we assume that it will be *quite* rare
            // we encode this using _two_ metadata entries 
            _metadata.Add((byte)255); // indication on varint encoded batch
            _metadata.Add((byte)128); // count of items to read
            var prevIdx = Math.Max(0, i - 1);
            totalSize += ComputeVarintDeltaSize(entries[prevIdx], entries + i, 128, prefixShift);
            _metadata.Add((byte)255); // another varint enocded batch
            _metadata.Add((byte)128); // second items count
            totalSize += ComputeVarintDeltaSize(entries[prevIdx + 127], entries + i + 128, 128, prefixShift);
        }
    }

    private static int ComputeVarintDeltaSize(long previous, long* buffer, int count, int shift)
    {
        previous >>>= shift;
        var size = 0;
        for (int i = 0; i < count; i++)
        {
            var cur = buffer[i] >>> shift;
            size += (64 - BitOperations.LeadingZeroCount((ulong)(cur - previous) | 1) + 6) / 7;
            previous = cur;
        }

        return size;
    }

    public (int Count, int SizeUsed) Write(byte* output, int outputSize)
    {
        Debug.Assert(outputSize <= ushort.MaxValue, "Output buffer too large, we use ushort for offsets and don't want to overflow");

        var sizeUsed = sizeof(PForHeader);
        if (sizeUsed > outputSize)
            return default;

        ref var header = ref Unsafe.AsRef<PForHeader>(output);

        header.SharedPrefix = _sharedPrefix;

        var baseline = _entries[Math.Max(0, _offset - 1)];
        if (header.SharedPrefix < (1 << PrefixSizeBits))
        {
            baseline >>= PrefixSizeBits;
        }

        header.Baseline = baseline;
        var exceptionsCounts = stackalloc int[33];
        var startingMetadataPosition = _metadataPos;

        var exceptionsRequiredSize = 0;
        var entriesOutput = (uint*)_entriesOutput;
        var oldOffset = _offset;
        while (_metadataPos < _metadata.Count)
        {
            var batchMetadataStart = _metadataPos;
            var numOfBits = _metadata[_metadataPos++];

            if (numOfBits == 255) // varint batch
            {
                var sizeOfVarintBatch = _metadata[_metadataPos++];
                var used = WriteVarintBatch(sizeOfVarintBatch, output + sizeUsed, outputSize - sizeUsed);
                if (used == 0)
                {
                    _metadataPos = batchMetadataStart;
                    break;
                }
                _offset += sizeOfVarintBatch;
                sizeUsed += used;
                continue;
            }

            var numOfExceptions = _metadata[_metadataPos++];

            var reqSize = numOfBits * Vector256<byte>.Count;

            if (numOfExceptions > 0)
            {
                var maxNumOfBits = _metadata[_metadataPos++];
                var exceptionIndex = maxNumOfBits - numOfBits;
                var oldCount = exceptionsCounts[exceptionIndex];
                var newCount = oldCount + numOfExceptions;
                exceptionsCounts[exceptionIndex] = newCount;
                if (oldCount == 0)
                {
                    exceptionsRequiredSize += sizeof(ushort); // size for the number of items here
                }

                exceptionsRequiredSize -= BitPacking.RequireSizeSegmented(oldCount, maxNumOfBits);
                exceptionsRequiredSize += BitPacking.RequireSizeSegmented(newCount, maxNumOfBits);
            }

            _metadataPos += numOfExceptions;
            var metaSize = _metadataPos - startingMetadataPosition;

            var finalSize = (sizeUsed + reqSize + exceptionsRequiredSize + metaSize);
            if (finalSize > outputSize)
            {
                _metadataPos = batchMetadataStart;
                break;
            }
            SimdBitPacking<MaskEntries>.Pack256(0, entriesOutput + _offset,
                output + sizeUsed, numOfBits, new MaskEntries((1u << numOfBits) - 1));
            sizeUsed += reqSize;
            _offset += 256;
        }

        uint bitmap = 0;
        header.ExceptionsOffset = checked((ushort)sizeUsed);

        for (int numOfBits = 2; numOfBits <= 32; numOfBits++)
        {
            var count = exceptionsCounts[numOfBits];
            if (count == 0)
                continue;

            bitmap |= 1u << numOfBits - 1;
            Unsafe.Write(output + sizeUsed, (ushort)count);
            sizeUsed += sizeof(ushort);
            var span = CollectionsMarshal.AsSpan(_exceptions[numOfBits]);
            var exceptionStart = _exceptionsStart[numOfBits];
            span = span[exceptionStart..(exceptionStart + count)];
            fixed (uint* b = span)
            {
                sizeUsed += BitPacking.PackSegmented(b, span.Length, output + sizeUsed, (uint)numOfBits);
            }
            _exceptionsStart[numOfBits] += count;
        }

        header.ExceptionsBitmap = bitmap;
        header.MetadataOffset = checked((ushort)sizeUsed);

        var metadataSize = (_metadataPos - startingMetadataPosition);
        var metadataSpan = CollectionsMarshal.AsSpan(_metadata);
        var metadataBlockRange = metadataSpan[startingMetadataPosition..(startingMetadataPosition + metadataSize)];
        metadataBlockRange.CopyTo(new Span<byte>(output + sizeUsed, metadataSize));

        sizeUsed += metadataSize;

        return (_offset - oldOffset, sizeUsed);
    }

    private int WriteVarintBatch(int count, byte* output, int outputSize)
    {
        Debug.Assert(count <= 256);
        var buffer = stackalloc byte[256 * 10]; // 10 bytes per max int64 varint

        var shift = _sharedPrefix >= 1 << PrefixSizeBits ? 0 : 10;
        var prevIdx = Math.Max(0, _offset - 1);
        var previous = _entries[prevIdx] >> shift;
        var dest = buffer;
        for (int i = 0; i < count; i++)
        {
            var cur = _entries[i + _offset] >> shift;
            var delta = cur - previous;
            previous = cur;

            while (delta >= 0x80)
            {
                *dest++ = (byte)(delta | 0x80);
                delta >>= 7;
            }
            *dest++ = (byte)(delta);
        }

        var size = (dest - buffer);
        if (size > outputSize)
            return 0;

        Unsafe.CopyBlock(output, buffer, (uint)size);
        return (int)size;
    }

    private int ProcessBlock(uint* currentEntries)
    {
        var (bestB, maxB, exceptionCount) = FindBestBitWidths(currentEntries);
        _metadata.Add((byte)bestB);
        _metadata.Add((byte)exceptionCount);
        if (exceptionCount > 0)
        {
            _metadata.Add((byte)maxB);
        }

        uint maxVal = 1u << bestB;
        ref var exceptionBuffer = ref _exceptions[maxB - bestB];

        for (int j = 0; j < 256; j++)
        {
            if (currentEntries[j] >= maxVal)
            {
                var exList = _exceptions[maxB - bestB] ??= new();
                exList.Add(currentEntries[j] >>> bestB);
                _metadata.Add((byte)j);
            }
        }

        return bestB * Vector256<byte>.Count;
    }

    private static (int BestBitWidth, int MaxBitWidth, int ExceptionsCount) FindBestBitWidths(uint* entries)
    {
        const int blockSize = 256;
        const int exceptionOverhead = 8;

        var freqs = stackalloc int[33];
        for (int i = 0; i < blockSize; i++)
        {
            freqs[32 - BitOperations.LeadingZeroCount(entries[i])]++;
        }
        var bestBitWidth = 32;
        while (freqs[bestBitWidth] == 0 && bestBitWidth > 0)
        {
            bestBitWidth--;
        }
        var maxBitWidth = bestBitWidth;
        var bestCost = bestBitWidth * blockSize;
        var bestExceptionCount = 0;
        var exceptionsCount = 0;


        for (var curBitWidth = bestBitWidth - 1; curBitWidth >= 0; curBitWidth--)
        {
            var currentExceptions = freqs[curBitWidth + 1];
            exceptionsCount += currentExceptions;

            var curCost = exceptionsCount * exceptionOverhead +
                          exceptionsCount * (maxBitWidth - curBitWidth) +
                          curBitWidth * blockSize;

            if (curCost < bestCost)
            {
                bestCost = curCost;
                bestBitWidth = curBitWidth;
                bestExceptionCount = exceptionsCount;
            }
        }

        return (bestBitWidth, maxBitWidth, bestExceptionCount);
    }


    private (ushort Prefix, int Shift) RemoveSharedPrefix()
    {
        var maskScalar = (1L << PrefixSizeBits) - 1;
        var mask = Vector256.Create<long>(maskScalar);
        var prefixScalar = *_entries & maskScalar;
        var prefix = Vector256.Create(prefixScalar);
        int i = 0;
        var output = (long*)_entriesOutput;
        for (; i + Vector256<long>.Count <= _count; i += Vector256<long>.Count)
        {
            var cur = Vector256.Load(_entries + i);
            if ((cur & mask) != prefix)
            {
                return NoSharedPrefix();
            }
            Vector256.ShiftRightLogical(cur, PrefixSizeBits).Store(output + i);
        }

        for (; i < _count; i++)
        {
            var cur = _entries[i];
            if ((cur & maskScalar) != prefixScalar)
            {
                return NoSharedPrefix();
            }
            output[i] = cur >>> PrefixSizeBits;
        }

        return ((ushort)prefixScalar, 10);

        (ushort, int) NoSharedPrefix()
        {
            Unsafe.CopyBlock(_entriesOutput, _entries, (uint)(sizeof(long) * _count));
            return (ushort.MaxValue, 0); // invalid value, since > 10 bits
        }

    }

    public void Dispose()
    {
        _entriesOutputScope.Dispose();
    }
}
