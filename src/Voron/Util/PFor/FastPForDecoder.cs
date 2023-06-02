using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Sparrow.Compression;
using Sparrow.Server;

namespace Voron.Util.PFor;

public unsafe struct FastPForDecoder : IDisposable
{
    private const int PrefixSizeBits = FastPForEncoder.PrefixSizeBits;
    public bool IsValid => _input != null;
    
    private byte* _input;
    private byte* _metadata;
    private readonly byte* _end;
    private readonly uint* _exceptions;
    private fixed int _exceptionOffsets[32];
    private readonly int _prefixShiftAmount;
    private readonly ushort _sharedPrefix;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _exceptionsScope;
    private Vector256<long> _prev;

    public FastPForDecoder(ByteStringContext allocator , byte* input, int size)
    {
        _input = input;
        _end = input + size;
        ref var header = ref Unsafe.AsRef<PForHeader>(input);
        _metadata = input + header.MetadataOffset;
 
        if (header.SharedPrefix >= 1 << PrefixSizeBits)
        {
            _sharedPrefix = 0;
            _prefixShiftAmount = 0;
        }
        else
        {
            _prefixShiftAmount = PrefixSizeBits;
            _sharedPrefix = header.SharedPrefix;
        }

        _exceptions = null;
        var exceptionsBufferSize = 1024;
        var exceptionBufferOffset = 0;

        _exceptionsScope = allocator.Allocate(exceptionsBufferSize * sizeof(uint), out ByteString bs);
        _exceptions = (uint*)bs.Ptr;

        var exception = _input + header.ExceptionsOffset;
        for (int i = 2; i <= 32; i++)
        {
            if ((header.ExceptionsBitmap & (1 << (i - 1))) == 0)
                continue;

            var count = Unsafe.Read<ushort>(exception);
            exception += sizeof(ushort);

            if (count + exceptionBufferOffset > exceptionsBufferSize)
            {
                exceptionsBufferSize *= 2;
                allocator.GrowAllocation(ref bs, ref _exceptionsScope, exceptionsBufferSize * sizeof(uint));
                _exceptions = (uint*)bs.Ptr;
            }

            BitPacking.UnpackSegmented(exception, count, _exceptions + exceptionBufferOffset, (uint)i);
            _exceptionOffsets[i] = exceptionBufferOffset;
            exceptionBufferOffset += count;

            exception += BitPacking.RequireSizeSegmented(count, i);
        }

        _input += sizeof(PForHeader);
        _prev = Vector256.Create(header.Baseline);

    }

    public int Read(long* output, int outputCount)
    {
        var prefixAmount = _prefixShiftAmount;
        var sharedPrefixMask = Vector256.Create<long>(_sharedPrefix);

        var bigDeltaOffsetsBuffer = stackalloc byte*[64];
        int bigDeltaBufferUsed = 0;
        var buffer = stackalloc uint[256];
        int read = 0;
        while (_metadata < _end && read < outputCount)
        {
            Debug.Assert(read + 256 <= outputCount, "We assume a minimum of 256 free spaces");

            var numOfBits = *_metadata++;
            switch (numOfBits)
            {
                case FastPForEncoder.BiggerThanMaxMarker:
                    bigDeltaOffsetsBuffer[bigDeltaBufferUsed++] = _metadata;
                    _metadata += 17; // batch location + 16 bytes high bits of the delta
                    continue;
                case FastPForEncoder.VarIntBatchMarker:
                    var countOfVarIntBatch = *_metadata++;
                    var prevScalar = _prev.GetElement(3);
                    for (int i = 0; i < countOfVarIntBatch; i++)
                    {
                        var cur = VariableSizeEncoding.Read<long>(_input, out var offset);
                        _input += offset;
                        cur += prevScalar;
                        output[read++] = cur << _prefixShiftAmount | _sharedPrefix;
                        prevScalar = cur;
                    }
                    _prev = Vector256.Create(prevScalar);
                    continue;
                case > 32 and < FastPForEncoder.BiggerThanMaxMarker:
                    throw new ArgumentOutOfRangeException("Unknown bits amount: " + numOfBits);
            }

            var numOfExceptions = *_metadata++;

            SimdBitPacking<NoTransform>.Unpack256(0, _input, buffer, numOfBits);
            _input += numOfBits * Vector256<byte>.Count;

            if (numOfExceptions > 0)
            {
                var maxNumOfBits = *_metadata++;
                var bitsDiff = maxNumOfBits - numOfBits;
                if (bitsDiff == 1)
                {
                    var mask = 1u << numOfBits;
                    for (int i = 0; i < numOfExceptions; i++)
                    {
                        var idx = *_metadata++;
                        buffer[idx] |= mask;
                    }
                }
                else
                {
                    ref var offset = ref _exceptionOffsets[bitsDiff];
                    for (int i = 0; i < numOfExceptions; i++)
                    {
                        var remains = _exceptions[offset++];
                        var idx = *_metadata++;
                        buffer[idx] |= remains << numOfBits;
                    }
                }
            }

            var expectedBufferIndex = -1;
            var deltaBufferIndex = 0;
            if (deltaBufferIndex < bigDeltaBufferUsed)
            {
                expectedBufferIndex = *bigDeltaOffsetsBuffer[deltaBufferIndex]++;
            }

            for (int i = 0; i + Vector256<uint>.Count <= 256; i += Vector256<uint>.Count)
            {
                var (a, b) = Vector256.Widen(Vector256.Load(buffer + i).AsInt32());
                if (expectedBufferIndex == i)
                {
                    a |= GetDeltaHighBits();
                }
                PrefixSumAndStoreToOutput(a, ref _prev);
                if (expectedBufferIndex == i + 4)
                {
                    b |= GetDeltaHighBits();
                }
                PrefixSumAndStoreToOutput(b, ref _prev);
            }
            bigDeltaBufferUsed = 0;

            Vector256<long> GetDeltaHighBits()
            {
                var highBitsDelta = Vector128.Load(bigDeltaOffsetsBuffer[deltaBufferIndex])
                    .AsInt32()
                    .ToVector256();
                deltaBufferIndex++;
                if (deltaBufferIndex < bigDeltaBufferUsed)
                {
                    expectedBufferIndex = *bigDeltaOffsetsBuffer[deltaBufferIndex]++;
                }
                else
                {
                    expectedBufferIndex = -1;
                }

                highBitsDelta = Vector256.Shuffle(highBitsDelta, Vector256.Create(3, 0, 4, 0, 5, 0, 6, 0));
                return highBitsDelta.AsInt64();
            }
        }

        return read;

        void PrefixSumAndStoreToOutput(Vector256<long> cur, ref Vector256<long> prev)
        {
            // doing prefix sum here: https://en.algorithmica.org/hpc/algorithms/prefix/
            cur += Vector256.Shuffle(cur, Vector256.Create(0, 0, 1, 2)) &
                   Vector256.Create(0, -1, -1, -1);
            cur += Vector256.Shuffle(cur, Vector256.Create(0, 0, 0, 1)) &
                   Vector256.Create(0, 0, -1, -1);
            cur += prev;
            prev = Vector256.Shuffle(cur, Vector256.Create(3, 3, 3, 3));
            cur = Vector256.ShiftLeft(cur, prefixAmount) | sharedPrefixMask;
            cur.Store(output + read);
            read += Vector256<long>.Count;
        }
    }

    public void Dispose()
    {
        _exceptionsScope.Dispose();
    }
}
