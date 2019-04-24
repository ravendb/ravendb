using System;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;

namespace Raven.Server.Documents.TimeSeries
{
    public unsafe struct TimeSeriesValuesSegment 
    {
        public const int BitsForTagLen = 11;

        public const int BitsForFirstTimestamp = 31;
        public const int LeadingZerosLengthBits = 5;
        public const int BlockSizeAdjustment = 1;
        public const int DefaultDelta = 0;

        public const int MaxLeadingZerosLength = (1 << LeadingZerosLengthBits) - 1;
        public const int BlockSizeLengthBits = 6;

        private byte* _buffer;
        private int _capacity;
        private SegmentHeader* Header => (SegmentHeader*)_buffer;


        public DateTime GetLastTimestamp(DateTime baseline)
        {
            return baseline.AddMilliseconds(Header->PreviousTimeStamp);
        }
        
        public int NumberOfBytes
        {
            get
            {
                var bitsHeader = GetBitsBuffer();
                var bytes = bitsHeader.NumberOfBits / 8 + (bitsHeader.NumberOfBits % 8 == 0 ? 0 : 1);

                return bytes + sizeof(BitsBufferHeader) + DataStart;
            }
        }
        
        

        public TimeSeriesValuesSegment(byte* buffer, int capacity)
        {
            _buffer = buffer;
            _capacity = capacity;

            if (_capacity > 2048)
                InvalidCapacity();
        }

        private void InvalidCapacity()
        {
            throw new ArgumentOutOfRangeException("Maximum capacity for segment is 2KB, but was: " + _capacity);
        }

        private int DataStart => sizeof(SegmentHeader) + sizeof(StatefulTimeStampValue) * Header->NumberOfValues;
        
        public int NumberOfValues => Header->NumberOfValues;

        public int NumberOfEntries => Header->NumberOfEntries;

        public void Initialize(int numberOfValues)
        {
            new Span<byte>(_buffer, _capacity).Clear();
            if (numberOfValues > 32)
                ThrowValuesOutOfRange(numberOfValues);
            if (_capacity > ushort.MaxValue)
                ThrowInvalidCapacityLength();

            Header->NumberOfValues = (byte)numberOfValues;
        }

        public bool Append(ByteStringContext allocator, int deltaFromStart, double val, Span<byte> tag)
        {
            return Append(allocator, deltaFromStart, new Span<double>(&val, 1), tag);
        }

        public bool Append(ByteStringContext allocator, int deltaFromStart, Span<double> vals, Span<byte> tag)
        {
            if (vals.Length != Header->NumberOfValues)
                ThrowInvalidNumberOfValues(vals);
            if (tag.Length > byte.MaxValue)
                ThrowInvalidTagLength();

            var actualBitsBuffer = GetBitsBuffer();

            var maximumSize = 
                sizeof(BitsBufferHeader) +
                sizeof(int) + // max timestamp
                sizeof(double) * vals.Length +
                tag.Length + 1 + 
                2 + // previous tag position
                1  // alignment to current buffer
#if DEBUG
                + 8 // extra buffer that should never be used
#endif
                ;

            using (allocator.Allocate(maximumSize + sizeof(SegmentHeader), out var tempBuffer))
            {
                Memory.Set(tempBuffer.Ptr, 0, maximumSize);
#if DEBUG
                *(ulong*)(tempBuffer.Ptr + maximumSize - sizeof(ulong)) = 0xdeadbeafdeadbeaf;
#endif

                var tempHeader = (SegmentHeader*)(tempBuffer.Ptr + maximumSize);

                *tempHeader = *Header;

                var tempBitsBuffer = new BitsBuffer(tempBuffer.Ptr, maximumSize);

                var prevs = new Span<StatefulTimeStampValue>(_buffer + sizeof(SegmentHeader), Header->NumberOfValues);
                AddTimeStamp(deltaFromStart, ref tempBitsBuffer, tempHeader);

                for (int i = 0; i < vals.Length; i++)
                {
                    AddValue(ref prevs[i], ref tempBitsBuffer, vals[i]);
                }

                WriteTag(tag, ref tempBitsBuffer, tempHeader, actualBitsBuffer.NumberOfBits);

#if DEBUG
                Debug.Assert(tempBitsBuffer.NumberOfBits / 8 < maximumSize, "Wrote PAST END OF BUFFER!");
                Debug.Assert(*(ulong*)(tempBuffer.Ptr + maximumSize - sizeof(ulong)) == 0xdeadbeafdeadbeaf, "Wrote TO END OF BUFFER");
#endif

                tempHeader->PreviousTimeStamp = deltaFromStart;
                tempHeader->NumberOfEntries++;

                if (actualBitsBuffer.AddBits(tempBitsBuffer) == false)
                    return false;

                *Header = *tempHeader;

                return true;
            }
        }

        private void WriteTag(Span<byte> tag, ref BitsBuffer tempBitsBuffer, SegmentHeader* tempHeader, int baseNumberOfBits)
        {
            var actualBitsBuffer = GetBitsBuffer();
            var tagEnum = new TagEnumerator(actualBitsBuffer /* need to read the previous values */, 
                tempHeader->PreviousTagPosition);
            if (tagEnum.TryGetPrevious(out var prevTag, out var previousIndex))
            {
                if (prevTag.SequenceEqual(tag))
                {
                    tempBitsBuffer.AddValue(0, 1); // reuse previous buffer
                    return;
                }
                // go back a maximum of 8 tags, to avoid N**2 operations
                // after 8 tags, we'll just write the tag again
                for (int i = 0; i < 8; i++)
                {
                    if (tagEnum.TryGetPrevious(out prevTag, out previousIndex) == false)
                        break;
                    if (prevTag.SequenceEqual(tag))
                    {
                        tempBitsBuffer.AddValue(1, 1);
                        tempBitsBuffer.AddValue((ulong)previousIndex, BitsForTagLen);
                        if (previousIndex != 0)
                        {
                            // re-arrange the previous pointer for the previous value
                            // to point to the current one
                            actualBitsBuffer.SetBits(
                                (previousIndex + 1 + tag.Length) * 8,
                                tempHeader->PreviousTagPosition,
                                BitsForTagLen
                                );
                            tempHeader->PreviousTagPosition = (ushort)previousIndex;
                        }
                        return;
                    }
                }
            }
            tempBitsBuffer.AddValue(1, 1);

            int currentBitPosition = baseNumberOfBits + tempBitsBuffer.NumberOfBits + BitsForTagLen;
            int currentTagPosition = GetByteIndex(currentBitPosition);
            var bitsToSkip = ToByteAlignment(currentBitPosition);

            tempBitsBuffer.AddValue((ulong)currentTagPosition, BitsForTagLen);


            tempBitsBuffer.Header->BitsPosition += (ushort)bitsToSkip;

            tempBitsBuffer.AddValue((ulong)tag.Length, 8);
            for (int i = 0; i < tag.Length; i++)
            {
                tempBitsBuffer.AddValue(tag[i], 8);
            }
            tempBitsBuffer.AddValue(tempHeader->PreviousTagPosition, BitsForTagLen);
            tempHeader->PreviousTagPosition = (ushort)(currentTagPosition);
        }

        private static int GetByteIndex(int numberOfBits)
        {
            var currentTagPosition = numberOfBits;
            currentTagPosition += ToByteAlignment(currentTagPosition);
            currentTagPosition /= 8;// the tag position is in _bytes_ - 0 .. 2048
            return currentTagPosition;
        }

        private static void ThrowInvalidTagLength()
        {
            throw new ArgumentOutOfRangeException("TimeSeries tag value cannot exceed 256 bytes");
        }

        private static ushort ToByteAlignment(int bits)
        {
            var mod = bits % 8;
            if (mod == 0)
                return 0;
            return (ushort)(8 - mod);
        }


        public ref struct TagEnumerator
        {
            BitsBuffer _bitsBuffer;
            int _previousTagPos;

            public TagEnumerator(BitsBuffer bitsBuffer, int previousTagPos)
            {
                _bitsBuffer = bitsBuffer;
                _previousTagPos = previousTagPos;
            }

            public bool TryGetPrevious(out Span<byte> tag, out int previousIndex)
            {
                if(_previousTagPos == 0)
                {
                    tag = default;
                    previousIndex = default;
                    return false;
                }
                previousIndex = _previousTagPos;
                var tagLen = _bitsBuffer.Buffer[_previousTagPos++];
                tag = new Span<byte>(_bitsBuffer.Buffer + _previousTagPos, tagLen);
                var offset = (_previousTagPos + tag.Length) * 8;
                _previousTagPos = (int)_bitsBuffer.ReadValue(ref offset, BitsForTagLen);
                return true;
            }
        }

        [Pure]
        public BitsBuffer GetBitsBuffer() => new BitsBuffer(_buffer + DataStart, _capacity - DataStart - sizeof(BitsBufferHeader));

        private static void AddTimeStamp(int deltaFromStart, ref BitsBuffer bitsBuffer, SegmentHeader* tempHeader)
        {
            if (tempHeader->NumberOfEntries == 0)
            {
                bitsBuffer.AddValue((ulong)deltaFromStart, BitsForFirstTimestamp);
                tempHeader->PreviousDelta = DefaultDelta;
                return;
            }

            int delta = deltaFromStart - tempHeader->PreviousTimeStamp;
            int deltaOfDelta = delta - tempHeader->PreviousDelta;
            if (deltaOfDelta == 0)
            {
                bitsBuffer.AddValue(0, 1);
                return;
            }
            if (deltaOfDelta > 0)
            {
                // There are no zeros. Shift by one to fit in x number of bits
                deltaOfDelta--;
            }

            int absValue = Math.Abs(deltaOfDelta);
            foreach (var timestampEncoding in TimestampEncodingDetails.Encodings)
            {
                if (absValue < timestampEncoding.MaxValueForEncoding)
                {
                    bitsBuffer.AddValue((ulong)timestampEncoding.ControlValue, timestampEncoding.ControlValueBitLength);

                    // Make this value between [0, 2^timestampEncodings[i].bitsForValue - 1]
                    long encodedValue = deltaOfDelta + timestampEncoding.MaxValueForEncoding;
                    bitsBuffer.AddValue((ulong)encodedValue, timestampEncoding.BitsForValue);

                    break;
                }
            }
            tempHeader->PreviousTimeStamp = deltaFromStart;
            tempHeader->PreviousDelta = delta;
        }

        private static void AddValue(ref StatefulTimeStampValue prev, ref BitsBuffer bitsBuffer, double dblVal)
        {
            long val = BitConverter.DoubleToInt64Bits(dblVal);
            ulong xorWithPrevious = (ulong)(prev.LongValue ^ val);
            if (xorWithPrevious == 0)
            {
                // It's the same value.
                bitsBuffer.AddValue(0, 1);
                return;
            }

            bitsBuffer.AddValue(1, 1);

            var leadingZeroes = Bits.LeadingZeroes(xorWithPrevious);
            var trailingZeroes = Bits.TrailingZeroesInBytes(xorWithPrevious);
      
            if (leadingZeroes > MaxLeadingZerosLength)
                leadingZeroes = MaxLeadingZerosLength;

            var useful = 64 - leadingZeroes - trailingZeroes;
            var prevUseful = 64 - prev.LeadingZeroes - prev.TrailingZeroes;

            var expectedSize = LeadingZerosLengthBits + BlockSizeLengthBits + useful;
            if (leadingZeroes >= prev.LeadingZeroes &&
                trailingZeroes >= prev.TrailingZeroes &&
                prevUseful < expectedSize)
            {
                // Control bit saying we should use the previous block information
                bitsBuffer.AddValue(1, 1);

                // Write the parts of the value that changed.
                ulong blockValue = xorWithPrevious >> prev.TrailingZeroes;
                bitsBuffer.AddValue(blockValue, prevUseful);
            }
            else
            {
                bitsBuffer.AddValue(0, 1);
                bitsBuffer.AddValue((ulong)leadingZeroes, LeadingZerosLengthBits);
                bitsBuffer.AddValue((ulong)(useful - BlockSizeAdjustment), BlockSizeLengthBits);
                ulong blockValue = xorWithPrevious >> trailingZeroes;
                bitsBuffer.AddValue(blockValue, useful);
                prev.LeadingZeroes = (byte)leadingZeroes;
                prev.TrailingZeroes = (byte)trailingZeroes;
            }
            prev.DoubleValue = dblVal;
        }

        public Enumerator GetEnumerator() => new Enumerator(this);

        public struct TagPointer
        {
            public byte* Pointer;
            public int Length;

            public Span<byte> AsSpan()
            {
                return new Span<byte>(Pointer, Length);
            }
        }

        public struct Enumerator
        {
            private readonly TimeSeriesValuesSegment _parent;
            private int _bitsPosisition;
            private int _previousTimeStamp, _previousTimeStampDelta;

            public Enumerator(TimeSeriesValuesSegment parent)
            {
                _parent = parent;
                _bitsPosisition = 0;
                _previousTimeStamp = _previousTimeStampDelta = -1;
            }

            public bool MoveNext(out int timestamp, Span<double> values, Span<TimeStampState> state, ref TagPointer tag)
            {
                if (values.Length != _parent.Header->NumberOfValues)
                    ThrowInvalidNumberOfValues();

                var bitsBuffer = _parent.GetBitsBuffer();

                if (_bitsPosisition >= bitsBuffer.NumberOfBits)
                {
                    timestamp = default;
                    return false;
                }
                if (_bitsPosisition == 0)
                {
                    // we use the values as the statement location for the previous values as well
                    values.Clear();
                    state.Clear();
                    tag = default;
                }

                timestamp = ReadTimeStamp(bitsBuffer);

                ReadValues(MemoryMarshal.Cast<double,long>(values), state, ref bitsBuffer);

                var reuseTag = bitsBuffer.ReadValue(ref _bitsPosisition, 1);
                if (reuseTag != 0)
                {
                    var tagPos = (int)bitsBuffer.ReadValue(ref _bitsPosisition, BitsForTagLen);
                    var nextTag = tagPos * 8 == _bitsPosisition + ToByteAlignment(_bitsPosisition);
                    var tagLen = (int)bitsBuffer.Buffer[tagPos++];
                    tag = new TagPointer
                    {
                        Pointer = bitsBuffer.Buffer + tagPos,
                        Length = tagLen
                    };
                    if (nextTag)
                    {
                        tagPos *= 8;
                        tagPos += tagLen * 8;
                        tagPos += BitsForTagLen; // skip over previous
                        _bitsPosisition = tagPos;
                    }
                }

                return true;
            }

            private void ReadValues(Span<long> values, Span<TimeStampState> state, ref BitsBuffer bitsBuffer)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    var nonZero = bitsBuffer.ReadValue(ref _bitsPosisition, 1);
                    if (nonZero == 0)
                    {
                        continue; // no change since last time
                    }
                    var usePreviousBlockInfo = bitsBuffer.ReadValue(ref _bitsPosisition, 1);
                    long xorValue;

                    if (usePreviousBlockInfo == 1)
                    {
                        xorValue = (long)bitsBuffer.ReadValue(ref _bitsPosisition, 64 - state[i].LeadingZeroes - state[i].TrailingZeroes);
                        xorValue <<= state[i].TrailingZeroes;
                    }
                    else
                    {
                        int leadingZeros = (int)bitsBuffer.ReadValue(ref _bitsPosisition, LeadingZerosLengthBits);
                        int blockSize = (int)bitsBuffer.ReadValue(ref _bitsPosisition, BlockSizeLengthBits) + BlockSizeAdjustment;
                        int trailingZeros = 64 - blockSize - leadingZeros;

                        xorValue = (long)bitsBuffer.ReadValue(ref _bitsPosisition, blockSize);
                        xorValue <<= trailingZeros;

                        state[i].TrailingZeroes = (byte)trailingZeros;
                        state[i].LeadingZeroes = (byte)leadingZeros;
                    }

                    values[i] = values[i] ^ xorValue;
                }
            }

            private int ReadTimeStamp(BitsBuffer bitsBuffer)
            {
                if (_bitsPosisition == 0)
                {
                    _previousTimeStamp = (int)bitsBuffer.ReadValue(ref _bitsPosisition, BitsForFirstTimestamp);
                    _previousTimeStampDelta = DefaultDelta;
                    return _previousTimeStamp;
                }

                var type = bitsBuffer.FindTheFirstZeroBit(ref _bitsPosisition, TimestampEncodingDetails.MaxControlBitLength);
                if (type > 0)
                {
                    var index = type - 1;
                    ref var encoding = ref TimestampEncodingDetails.Encodings[index];
                    long decodedValue = (long)bitsBuffer.ReadValue(ref _bitsPosisition, encoding.BitsForValue);
                    // [0, 255] becomes [-128, 127]
                    decodedValue -= encoding.MaxValueForEncoding;
                    if (decodedValue >= 0)
                    {
                        // [-128, 127] becomes [-128, 128] without the zero in the middle
                        decodedValue++;
                    }
                    _previousTimeStampDelta += (int)decodedValue;
                }
                _previousTimeStamp += _previousTimeStampDelta;

                return _previousTimeStamp;
            }

            private void ThrowInvalidNumberOfValues()
            {
                throw new ArgumentOutOfRangeException("The values span provided must have a length of exactly: " + _parent.Header->NumberOfValues);
            }
        }


        private void ThrowInvalidNumberOfValues(Span<double> vals)
        {
            throw new ArgumentOutOfRangeException("Expected to have " + Header->NumberOfValues + " values, but was provided with: " + vals.Length);
        }

        private void ThrowInvalidCapacityLength()
        {
            throw new ArgumentOutOfRangeException("TimeSeriesValuesSegment can handle a size of up to 65,535, but was: " + _capacity);
        }

        private static void ThrowValuesOutOfRange(int numberOfValues)
        {
            throw new ArgumentOutOfRangeException("TimeSeriesValuesSegment can handle up to 32 values, but had: " + numberOfValues);
        }



    }
}
