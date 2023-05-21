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
        private uint _prevValue;
        public byte* Offset;
        private byte* _segmentsBits;
        private int _segmentIndex;
        private SimdBitPackingHeader* _header;

        public void MoveToNextHeader()
        {
            _segmentsBits = Offset + sizeof(SimdBitPackingHeader);
            _header = (SimdBitPackingHeader*)Offset;
            Offset += _header->OffsetToFirstSegment;
            _segmentIndex = 0;
            _prevValue = 0;
        }

        /// <summary>
        /// Decodes entries using SIMD packing into the provided input buffer
        /// The buffer should be sized in 256 chunks. 
        /// </summary>
        [SkipLocalsInit]
        public int Fill(long* entries, int count)
        {
            Debug.Assert(count >= 256 && count % 256 == 0);
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
                SimdCompression<TSimdTransform>.Unpack256(_prevValue, Offset, uintBuffer, bits);
                var bufferSize = SimdCompression<TSimdTransform>.RequiredBufferSize(256, bits);
                _prevValue = uintBuffer[255];
                Offset += bufferSize;
                ConvertToInt64();
            }
            if (header.LastSegmentCount > 0 &&
                read + header.LastSegmentCount < count &&
                _segmentIndex == header.NumberOfFullSegments)
            {
                byte bits = _segmentsBits[_segmentIndex];
                SimdCompression<TSimdTransform>.UnpackSmall(_prevValue, Offset, header.LastSegmentCount,
                    uintBuffer, bits);
                _segmentIndex++; // this mark it as consumed, next call will not hit it
                read += header.LastSegmentCount;
                var bufferSize = SimdCompression<TSimdTransform>.RequiredBufferSize(header.LastSegmentCount , bits);
                Offset += bufferSize;
                for (int i = 0; i < header.LastSegmentCount; i++)
                {
                    *curEntries++ = ((long)*uintBuffer++ << header.ShiftAmount) + header.Baseline;
                }
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
        count = Math.Min(count, 256 * 128); // ensure we can't overflow NumberOfFullSegments
        if (entries[count - 1] - entries[0] > uint.MaxValue)
            return UnlikelySplitEntries();

        uint* uintBuffer = stackalloc uint[256];
        var countOfFullSegments = count / 256;

        // we ensure that we are always taking metadata that is 32 bytes in size
        var headerSize = (sizeof(SimdBitPackingHeader) + (count + 255) / 256 + 31) / 32 * 32;
        if (headerSize > outputSize)
            return default; // should never happen
        Debug.Assert(headerSize < byte.MaxValue);

        var header = (SimdBitPackingHeader*)output;
        var segmentsBits = (output + sizeof(SimdBitPackingHeader));
        var shiftAmount = ComputeSharedPrefix(entries, count, out header->Prefix);
        InitializeHeader();
        var baselineScalar = entries[0] >> shiftAmount;

        var sizeUsed = headerSize;

        var baseline = Vector256.Create(baselineScalar);
        uint prevValue = 0;

        for (var segmentIdx = 0; segmentIdx < countOfFullSegments; segmentIdx++)
        {
            ConvertFullSegmentToUint32();
            var bits = SimdCompression<TSimdTransform>.FindMaxBits(prevValue, uintBuffer, 256);
            var size = SimdCompression<TSimdTransform>.RequiredBufferSize(256, (int)bits);
            if (sizeUsed + size > outputSize) //        not enough space, return early
                return (segmentIdx * 256, sizeUsed); // should be rare
            SimdCompression<TSimdTransform>.Pack256(prevValue, uintBuffer, output + sizeUsed, bits);
            sizeUsed += size;
            prevValue = uintBuffer[255];
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
            if (sizeUsed + size > outputSize)
                return (countOfFullSegments * 256, sizeUsed);
            SimdCompression<TSimdTransform>.PackSmall(prevValue, uintBuffer, remainingItems, output + sizeUsed, bits);
            header->LastSegmentCount = (byte)remainingItems;
            sizeUsed += size;
            segmentsBits[countOfFullSegments] = (byte)bits;
        }
        return (count, sizeUsed);

        (int Count, int SizeUsed) UnlikelySplitEntries()
        {
            var entriesSpan = new Span<long>(entries, count);
            var sep = entriesSpan.BinarySearch(entries[0] + uint.MaxValue - 1);
            if (sep < 0)
                sep = ~sep;
            // we want to favor getting 256 exactly, and we have enough to do so
            var remainder = sep % 256;
            if (remainder != 0 && sep > remainder)
                sep -= remainder;
            RuntimeHelpers.EnsureSufficientExecutionStack();
            return Encode(entries, sep, output, outputSize);
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
            header->OffsetToFirstSegment = (byte)headerSize;
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
