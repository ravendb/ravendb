using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Sparrow.Server;

namespace Voron.Util.PFor;

public unsafe struct FastPForDecoder : IDisposable
{
    private const int PrefixSizeBits = FastPForEncoder.PrefixSizeBits;

    private byte* _input;
    private byte* _metadata;
    private readonly byte* _end;
    private readonly uint* _exceptions;
    private long _baseline;
    private fixed int _exceptionOffsets[32];
    private readonly int _prefixShiftAmount;
    private readonly ushort _sharedPrefix;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _exceptionsScope;

    public FastPForDecoder(ByteStringContext allocator , byte* input, int size)
    {
        _input = input;
        _end = input + size;
        ref var header = ref Unsafe.AsRef<PForHeader>(input);
        _metadata = input + header.MetadataOffset;
        _baseline = header.Baseline;
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

    }

    public int Read(long* output, int outputCount)
    {
        var prefixAmount = _prefixShiftAmount;
        var sharedPrefixMask = Vector256.Create<long>(_sharedPrefix);
        var prev = Vector256.Create(_baseline);

        var buffer = stackalloc uint[256];
        int read = 0;
        while (_metadata < _end && read < outputCount)
        {
            Debug.Assert(read + 256 <= outputCount, "We assume a minimum of 256 free spaces");

            var numOfBits = *_metadata++;

            if(numOfBits == 255) // either a batch with delta > uint.MaxValue or remainder of less than 256
            {
                var countOfVarintBatch = *_metadata++;
                var prevScalar = prev.GetElement(3);
                for (int i = 0; i < countOfVarintBatch; i++)
                {
                    var cur = ReadVarInt64(ref _input) + prevScalar;
                    output[read++] = cur << _prefixShiftAmount | _sharedPrefix;
                    prevScalar = cur;
                }
                prev = Vector256.Create(prevScalar);
                continue;
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

            for (int i = 0; i + Vector256<uint>.Count <= 256; i += Vector256<uint>.Count)
            {
                var (a, b) = Vector256.Widen(Vector256.Load(buffer + i).AsInt32());
                WriteToOutput(a);
                WriteToOutput(b);
            }
        }

        return read;

        void WriteToOutput(Vector256<long> cur)
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

    private long ReadVarInt64(ref byte* input)
    {
        const int MaxBytesWithoutOverflow = 9;
        byte byteReadJustNow;
        ulong result = 0;
        for (int shift = 0; shift < MaxBytesWithoutOverflow * 7; shift += 7)
        {
            byteReadJustNow = *input++;
            result |= (byteReadJustNow & 0x7Ful) << shift;

            if (byteReadJustNow <= 0x7Fu)
            {
                return (long)result; // early exit
            }
        }

        // Read the 10th byte. Since we already read 63 bits,
        // the value of this byte must fit within 1 bit (64 - 63),
        // and it must not have the high bit set.
        byteReadJustNow = *input++;
        if (byteReadJustNow > 0b_1u)
        {
            throw new FormatException("Bad 7 bit value");
        }

        result |= (ulong)byteReadJustNow << (MaxBytesWithoutOverflow * 7);
        return (long)result;
    }

    public void Dispose()
    {
        _exceptionsScope.Dispose();
    }
}
