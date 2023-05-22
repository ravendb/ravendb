using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace Voron.Util.Simd;

public static unsafe class SimdBitPacker<TSimdTransform>
    where TSimdTransform : struct, ISimdTransform
{
    public struct Reader
    {
        private byte* _offset;
        private byte* _segmentsBits;
        private readonly byte* _endOfData;
        private SimdBitPackingHeader* _header;
        private uint _prevValue;
        private int _segmentIndex;
        private bool _atEndOfSegment;

        public bool IsValid => _offset != null;
        
        public Reader(byte* start, int size)
        {
            _endOfData = start + size;
            Reset(start);
        }


        private void Reset(byte* currentOffset)
        {
            _header = (SimdBitPackingHeader*)currentOffset;
            _segmentsBits = currentOffset + _header->OffsetToMetadata;
            _offset = currentOffset + sizeof(SimdBitPackingHeader);
            _segmentIndex = 0;
            _prevValue = 0;
            _atEndOfSegment = false;
        }

        public int Fill(long* entries, int count)
        {
            if (!(count >= 256 && count % 256 == 0))
                throw new NotSupportedException("Buffer size must be divisible by 256, but was: " + count);
            
            var read = FillInternal(entries, count);
            if (_atEndOfSegment && _offset < _endOfData)
                Reset(_offset);
            return read;
        }

        /// <summary>
        /// Decodes entries using SIMD packing into the provided input buffer
        /// The buffer should be sized in 256 chunks. 
        /// </summary>
        [SkipLocalsInit]
        private int FillInternal(long* entries, int count)
        {
            uint* uintBuffer = stackalloc uint[256];
            var read = 0;
            var curEntries = entries;
            var header = *_header;
            var baselineVec = Vector256.Create(header.Baseline);
            for (;
                 _segmentIndex < header.NumberOfFullSegments && read < count;
                 _segmentIndex++, read += 256)
            {
                var bits = _segmentsBits[_segmentIndex];
                SimdCompression<TSimdTransform>.Unpack256(_prevValue, _offset, uintBuffer, bits);
                var bufferSize = SimdCompression<TSimdTransform>.RequiredBufferSize(256, bits);
                _prevValue = uintBuffer[255];
                _offset += bufferSize;
                ConvertToInt64();
            }

            if (_segmentIndex == header.NumberOfFullSegments && read + header.LastSegmentCount <= count)
            {
                var metadataSize = header.NumberOfFullSegments;
                if( header.LastSegmentCount > 0)
                {
                    metadataSize++;
                    byte bits = _segmentsBits[_segmentIndex];
                    SimdCompression<TSimdTransform>.UnpackSmall(_prevValue, _offset, header.LastSegmentCount,
                        uintBuffer, bits);
                    _segmentIndex++; // this mark it as consumed, next call will not hit it
                    read += header.LastSegmentCount;
                    var bufferSize = SimdCompression<TSimdTransform>.RequiredBufferSize(header.LastSegmentCount , bits);
                    _offset += bufferSize;
                    for (int i = 0; i < header.LastSegmentCount; i++)
                    {
                        *curEntries++ = ((long)*uintBuffer++ << header.ShiftAmount) + header.Baseline;
                    }
                }

                _atEndOfSegment = true;
                _offset += metadataSize;
            }
            return read;

            void ConvertToInt64()
            {
                var inputBuf = uintBuffer;
                for (int i = 0; i < (256 / Vector256<uint>.Count); i++)
                {
                    var (hiU, loU) = Vector256.Widen(Vector256.Load(inputBuf));

                    inputBuf += Vector256<uint>.Count;

                    var hi = Vector256.ShiftLeft(hiU.AsInt64(), header.ShiftAmount) + baselineVec;
                    hi.Store(curEntries);
                    curEntries += Vector256<long>.Count;

                    var lo = Vector256.ShiftLeft(loU.AsInt64(), header.ShiftAmount) + baselineVec;
                    lo.Store(curEntries);
                    curEntries += Vector256<long>.Count;
                }
            }
        }

    }

    /// <summary>
    /// Encodes entries using SIMD packing into the output provided
    /// * May encode *less* than what is provided, if there is not enough space
    ///   or if the data doesn't match align on 32 bits boundary properly
    /// * Entries are expected to be sorted
    /// </summary>
    [SkipLocalsInit]
    public static (int Count, int SizeUsed) Encode(long* entries, int count, byte* output, int outputSize)
    {
        const int maxNumberOfSegments = 128;
        count = Math.Min(count, 256 * maxNumberOfSegments); // ensure we can't overflow NumberOfFullSegments
        if (entries[count - 1] - entries[0] > uint.MaxValue)
            return UnlikelySplitEntries();

        uint* uintBuffer = stackalloc uint[256];
        var countOfFullSegments = count / 256;
        byte* segmentsBits = stackalloc byte[maxNumberOfSegments];

        var header = (SimdBitPackingHeader*)output;
        var shiftAmount = ComputeSharedPrefix(entries, count, out header->Prefix);
        InitializeHeader();
        var baselineScalar = entries[0] >> shiftAmount;

        var sizeUsed = sizeof(SimdBitPackingHeader);

        var baseline = Vector256.Create(baselineScalar);
        uint prevValue = 0;
        var segmentIdx = 0;
        var written = 0;
        for (; segmentIdx < countOfFullSegments; segmentIdx++)
        {
            ConvertFullSegmentToUint32();
            var bits = SimdCompression<TSimdTransform>.FindMaxBits(prevValue, uintBuffer, 256);
            var size = SimdCompression<TSimdTransform>.RequiredBufferSize(256, (int)bits);
            var metadataBytes = segmentIdx + 1;
            if (sizeUsed + size + metadataBytes > outputSize) //        not enough space, return early
                return FinishEncoding();
            SimdCompression<TSimdTransform>.Pack256(prevValue, uintBuffer, output + sizeUsed, bits);
            sizeUsed += size;
            prevValue = uintBuffer[255];
            written += 256;
            header->NumberOfFullSegments++;
            segmentsBits[segmentIdx] = (byte)bits;
        }
        var remainingItems = count - (countOfFullSegments * 256);
        if (remainingItems > 0)
        {
            Debug.Assert(remainingItems <= byte.MaxValue);
            for (var i = 0; i < remainingItems; i++)
            {
                uintBuffer[i] = (uint)((entries[i] >> shiftAmount) - baselineScalar);
            }
            var bits = SimdCompression<TSimdTransform>.FindMaxBits(prevValue, uintBuffer, remainingItems);
            var size = SimdCompression<TSimdTransform>.RequiredBufferSize(remainingItems, (int)bits);
            var metadataBytes = segmentIdx + 1;
            if (sizeUsed + size + metadataBytes > outputSize)
                return FinishEncoding();
            written += remainingItems;
            SimdCompression<TSimdTransform>.PackSmall(prevValue, uintBuffer, remainingItems, output + sizeUsed, bits);
            header->LastSegmentCount = (byte)remainingItems;
            sizeUsed += size;
            segmentsBits[segmentIdx++] = (byte)bits;
        }
        return FinishEncoding();
        
        (int Count, int SizeUsed) FinishEncoding()
        {
            Unsafe.CopyBlock(output + sizeUsed, segmentsBits, (uint)segmentIdx);
            Debug.Assert(sizeUsed < ushort.MaxValue);
            header->OffsetToMetadata = (ushort)sizeUsed;
            sizeUsed += segmentIdx; // include the metadata usage
            return (written, sizeUsed);
        }
        
        (int Count, int SizeUsed) UnlikelySplitEntries()
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            var totalWritten = 0;
            var totalSize = 0;
            while (count > 0)
            {
                var entriesSpan = new Span<long>(entries, count);
                var sep = entriesSpan.BinarySearch(entries[0] + uint.MaxValue - 1);
                if (sep < 0)
                    sep = ~sep;
                // we want to favor getting 256 exactly, and we have enough to do so
                var remainder = sep % 256;
                if (remainder != 0 && sep > remainder)
                    sep -= remainder;

                (int w, int size)  = Encode(entries, sep, output, outputSize);
                totalSize += size;
                totalWritten += w;
                if (w != sep) // means that we run out of space in the buffer
                    break;
                entries += w;
                count -= w;
                output += size;
                outputSize -= size;
            }
            return (totalWritten, totalSize);
        }

        void ConvertFullSegmentToUint32()
        {
            var buf = uintBuffer;
            for (int i = 0; i + Vector256<long>.Count * 2 <= 256; i += Vector256<long>.Count * 2)
            {
                var high64 = Vector256.ShiftRightLogical(Vector256.Load(entries), shiftAmount) - baseline;
                entries += Vector256<long>.Count;
                var low64 = Vector256.ShiftRightLogical(Vector256.Load(entries), shiftAmount) - baseline;
                entries += Vector256<long>.Count;
                var v = Vector256.Narrow(high64, low64).AsUInt32();
                v.Store(buf);
                buf += Vector256<uint>.Count;
            }
        }

        void InitializeHeader()
        {
            header->Baseline = entries[0];
            header->OffsetToMetadata = 0;
            header->ShiftAmount = shiftAmount;
            header->LastSegmentCount = 0;
            header->NumberOfFullSegments = 0;
        }
    }

    private static byte ComputeSharedPrefix(long* entries, int count, out ushort prefix)
    {
        const ushort prefix13Bits = 0x1FFF;
        const byte shiftAmount = 13;
        
        prefix = (ushort)(entries[0] & prefix13Bits);
        var prefixMask = Vector256.Create<long>(prefix13Bits);
        var sharedPrefix = Vector256.Create<long>(prefix);
        var countOfSharedPrefixes = Vector256<long>.Zero;
        int i = 0;
        for (; i + Vector256<long>.Count <= count; i += Vector256<long>.Count)
        {
            countOfSharedPrefixes += Vector256.Equals((Vector256.Load(entries + i) & prefixMask), sharedPrefix);
        }
        var scalarCountOfSharedPrefixes = 0;
        for (; i < count; i++)
        {
            scalarCountOfSharedPrefixes -= BoolToInt((entries[i] & prefix13Bits) == prefix);
        }
        countOfSharedPrefixes += Vector256.Create<long>(scalarCountOfSharedPrefixes);

        var (vecExpectedCount, scalarExpectedCount) = Math.DivRem(count, Vector256<long>.Count);

        var expectedCount = Vector256.Create<long>(-vecExpectedCount - scalarExpectedCount);

        return countOfSharedPrefixes == expectedCount ? shiftAmount : (byte)0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int BoolToInt(bool v)
    {
        return *(int*)&v;
    }

}
