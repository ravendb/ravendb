using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.PostingLists;

namespace Voron.Util.PFor;

public unsafe struct FastPForDecoder : IDisposable
{
    private const int PrefixSizeBits = FastPForEncoder.PrefixSizeBits;
    public bool IsValid => _input != null;

    private readonly ByteStringContext _allocator;
    private byte* _input;
    private byte* _metadata;
    private byte* _end;
    private uint* _exceptions;
    private fixed int _exceptionOffsets[32];
    private int _prefixShiftAmount;
    private ushort _sharedPrefix;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _exceptionsScope;
    private Vector256<long> _prev;
    private ByteString _bs;

    public FastPForDecoder(ByteStringContext allocator)
    {
        _allocator = allocator;
        const int initialExceptionsSize = 1024;
        _exceptionsScope = allocator.Allocate(initialExceptionsSize * sizeof(uint), out _bs);
        _exceptions = (uint*)_bs.Ptr;
    }

    public void Init( byte* input, int size)
    {
        if (size <= sizeof(PForHeader)) throw new ArgumentOutOfRangeException(nameof(size));
        
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

        var exceptionBufferOffset = 0;
      
        var exception = _input + header.ExceptionsOffset;
        for (int i = 2; i <= 32; i++)
        {
            if ((header.ExceptionsBitmap & (1 << (i - 1))) == 0)
                continue;

            var count = Unsafe.Read<ushort>(exception);
            exception += sizeof(ushort);

            int sizeRequired = count + exceptionBufferOffset;
            if (sizeRequired > _bs.Length)
            {
                GrowAllocation(sizeRequired);
            }

            BitPacking.UnpackSegmented(exception, count, _exceptions + exceptionBufferOffset, (uint)i);
            _exceptionOffsets[i] = exceptionBufferOffset;
            exceptionBufferOffset += count;

            exception += BitPacking.RequireSizeSegmented(count, i);
        }

        _input += sizeof(PForHeader);
        _prev = Vector256.Create(header.Baseline);

    }

    private void GrowAllocation(int size)
    {
        do
        {
            _allocator.GrowAllocation(ref _bs, ref _exceptionsScope, _bs.Length * 2);
            _exceptions = (uint*)_bs.Ptr;
        } while (size > _bs.Length);
    }

    public int Read(long* output, int outputCount)
    {
        var prefixAmount = _prefixShiftAmount;
        var sharedPrefixMask = Vector256.Create<long>(_sharedPrefix);

        var bigDeltaOffsets = new NativeIntegersList(_allocator, -1);
        var buffer = stackalloc uint[256];
        int read = 0;
        while (_metadata < _end && read < outputCount)
        {
            Debug.Assert(read + 256 <= outputCount, "We assume a minimum of 256 free spaces");

            var numOfBits = *_metadata++;
            switch (numOfBits)
            {
                case FastPForEncoder.BiggerThanMaxMarker:
                    bigDeltaOffsets.Add((long)_metadata);
                    // we don't need to worry about block fit, because we are ensured that we have at least
                    // 256 items to read into the output here, and these marker are for the next blcok
                    
                    // batch location + 16 bytes high bits of the delta
                    _metadata += 17;
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
            if (bigDeltaOffsets.Count > 0)
            {
                expectedBufferIndex = *(byte*)bigDeltaOffsets.First;
            }

            for (int i = 0; i + Vector256<uint>.Count <= 256; i += Vector256<uint>.Count)
            {
                var (a, b) = Vector256.Widen(Vector256.Load(buffer + i));
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
            bigDeltaOffsets.Clear();

            Vector256<ulong> GetDeltaHighBits()
            {
                var ptr = (byte*)bigDeltaOffsets.Pop() + 1;
                var highBitsDelta = Vector128.Load(ptr)
                    .AsInt32().ToVector256();
                if (bigDeltaOffsets.Count > 0)
                {
                    expectedBufferIndex = *(byte*)bigDeltaOffsets.First;
                }
                else
                {
                    expectedBufferIndex = -1;
                }

                // the last 4 elements are known zero, so no need to AND with zero
                highBitsDelta = Vector256.Shuffle(highBitsDelta, Vector256.Create(7, 0, 7, 1, 7, 2, 7, 3));
                return highBitsDelta.AsUInt64();
            }
        }

        return read;

        void PrefixSumAndStoreToOutput(Vector256<ulong> curUl, ref Vector256<long> prev)
        {
            var cur = curUl.AsInt64();
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

    public static long ReadStart(byte* p)
    {
        return ((PForHeader*)p)->Baseline;
    }
}
