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
        public const int BitsForFirstTimestamp = 31;
        public const int LeadingZerosLengthBits = 5;
        public const int BlockSizeAdjustment = 1;
        public const int DefaultDelta = 0;

        public const int MaxLeadingZerosLength = (1 << LeadingZerosLengthBits) - 1;
        public const int BlockSizeLengthBits = 6;

        private byte* _buffer;
        private int _capacity;
        private SegmentHeader* Header => (SegmentHeader*)_buffer;

        public byte* Ptr => _buffer;
        public StatefulTimeStampValueSpan SegmentValues =>
            new StatefulTimeStampValueSpan(
                (StatefulTimeStampValue*)(_buffer + sizeof(SegmentHeader)),
                Header->NumberOfValues
            );

        public DateTime GetLastTimestamp(DateTime baseline)
        {
            return baseline.AddMilliseconds(Header->PreviousTimeStamp);
        }

        public int NumberOfBytes
        {
            get
            {
                return GetNumberOfBytes(Header);
            }
        }

        private int GetNumberOfBytes(SegmentHeader* header)
        {
            var bitsHeader = GetBitsBuffer(Header);

            return bitsHeader.NumberOfBytes + GetDataStart(Header);
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

        private static int GetDataStart(SegmentHeader* header) => sizeof(SegmentHeader) + sizeof(StatefulTimeStampValue) * header->NumberOfValues + header->SizeOfTags;

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
            Header->SizeOfTags = 1;
            Header->PreviousTagIndex = byte.MaxValue;// invalid tag value
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


            var maximumSize =
                sizeof(BitsBufferHeader) +
                sizeof(int) + // max timestamp
                sizeof(double) * vals.Length + vals.Length /* may use additional 2 bits per value here for the first items */ +
                2 // previous tag position (10 bits)
                ;

            using (allocator.Allocate(maximumSize + sizeof(SegmentHeader), out var tempBuffer))
            {
                Memory.Set(tempBuffer.Ptr, 0, maximumSize);

                var tempHeader = (SegmentHeader*)(tempBuffer.Ptr + maximumSize);

                *tempHeader = *Header;

                var tempBitsBuffer = new BitsBuffer(tempBuffer.Ptr, maximumSize);

                var prevs = new Span<StatefulTimeStampValue>(_buffer + sizeof(SegmentHeader), tempHeader->NumberOfValues);
                AddTimeStamp(deltaFromStart, ref tempBitsBuffer, tempHeader);

                if (tempHeader->NumberOfEntries == 0)
                {
                    for (int i = 0; i < vals.Length; i++)
                    {
                        prevs[i].Min = prevs[i].First = vals[i];
                    }
                }

                for (int i = 0; i < vals.Length; i++)
                {
                    AddValue(ref prevs[i], ref tempBitsBuffer, vals[i]);
                }
                bool insertTag = false;
                var prevTagIndex = FindPreviousTag(tag, tempHeader);
                if (prevTagIndex >= 0)
                {
                    if (Header->PreviousTagIndex == prevTagIndex)
                    {
                        tempBitsBuffer.AddValue(0, 1); // reuse previous buffer
                    }
                    else
                    {
                        tempBitsBuffer.AddValue(1, 1); // will write the tag index buffer here
                        if(prevTagIndex == byte.MaxValue)
                        {
                            tempBitsBuffer.AddValue(0, 1); // no tags here
                        }
                        else
                        {
                            tempBitsBuffer.AddValue(1, 1); // new value here
                            tempBitsBuffer.AddValue((ulong)prevTagIndex, 7);
                        }
                        tempHeader->PreviousTagIndex = (byte)prevTagIndex;
                    }
                }
                else
                {
                    // need to write the tag
                    insertTag = true;

                    tempBitsBuffer.AddValue(2, 2); // will write the tag index buffer here
                    tempBitsBuffer.AddValue((ulong)(~prevTagIndex), 8);
                    tempHeader->PreviousTagIndex = (byte)(~prevTagIndex);
                }

                tempHeader->PreviousTimeStamp = deltaFromStart;
                tempHeader->NumberOfEntries++;
                var actualBitsBuffer = GetBitsBuffer(tempHeader);

                if (insertTag)
                {
                    if (actualBitsBuffer.EnsureAdditionalBits(allocator, tempBitsBuffer.NumberOfBits + (tag.Length + 1) * 8) == false)
                        return false;

                    InsertTag(tag, tempHeader, actualBitsBuffer.NumberOfBytes);
                    actualBitsBuffer = GetBitsBuffer(tempHeader);
                    var result = actualBitsBuffer.AddBits(allocator, tempBitsBuffer);
                    Debug.Assert(result, "Failed to add bits, but already checked precondition");
                }
                else
                {
                    if (actualBitsBuffer.AddBits(allocator, tempBitsBuffer) == false)
                    {
                        return false;
                    }
                }


                *Header = *tempHeader;

                return true;
            }
        }

        /*
         * Tags are stored in the following manner
         * 
         * [raw bytes tag1], [raw bytes tag 2], etc
         * [len tag1], [len tag2], [total number of tags] : byte
         * 
         */

        public int FindPreviousTag(Span<byte> tag, SegmentHeader* tempHeader)
        {
            if (tag.IsEmpty)
                return byte.MaxValue;
            var offset = tempHeader->SizeOfTags;
            var tagsPtr = _buffer + sizeof(SegmentHeader) + sizeof(StatefulTimeStampValue) * tempHeader->NumberOfValues;
            var numberOfTags = *(tagsPtr + offset - 1);
            if (numberOfTags == 0)
                return ~0;
            var tagsLens = tagsPtr + offset - 1 - numberOfTags;
            var tagBackwardOffset = 0;
            for (int i = (numberOfTags - 1); i >= 0; i--)
            {
                tagBackwardOffset += tagsLens[i];
                var prevTag = new Span<byte>(tagsLens - tagBackwardOffset, tagsLens[i]);
                if (prevTag.SequenceEqual(tag))
                    return i;
            }

            return ~numberOfTags;
        }

        public void InsertTag(Span<byte> tag, SegmentHeader* tempHeader, int numberOfBytes)
        {
            var offset = tempHeader->SizeOfTags;
            var tagsPtr = _buffer + sizeof(SegmentHeader) + sizeof(StatefulTimeStampValue) * tempHeader->NumberOfValues;
            var numberOfTags = tagsPtr[offset - 1];

            if (numberOfTags >= 127)
                throw new InvalidOperationException("Cannot have more than 127 tags per segment");

            var tagsLens = tagsPtr + offset - 1 - numberOfTags;
            var copyOfTagLens = stackalloc byte[numberOfTags];
            Memory.Copy(copyOfTagLens, tagsLens, numberOfTags);
            var newEndOfTagsArray = tagsPtr + offset + tag.Length + 1;

            Debug.Assert((_capacity - (int)(newEndOfTagsArray - _buffer)) > numberOfBytes);
            Memory.Move(newEndOfTagsArray, tagsPtr + offset, numberOfBytes);
            tag.CopyTo(new Span<byte>(tagsLens, tag.Length));
            Memory.Copy(tagsLens + tag.Length, copyOfTagLens, numberOfTags);
            tagsLens += tag.Length + numberOfTags;

            *tagsLens++ = (byte)tag.Length;
            *tagsLens++ = (byte)(numberOfTags + 1);
            tempHeader->SizeOfTags = (ushort)(tagsLens - tagsPtr);
        }

        private static void ThrowInvalidTagLength()
        {
            throw new ArgumentOutOfRangeException("TimeSeries tag value cannot exceed 256 bytes");
        }

        [Pure]
        public BitsBuffer GetBitsBuffer(SegmentHeader* header)
        {
            int dataStart = GetDataStart(header);
            return new BitsBuffer(_buffer + dataStart, _capacity - dataStart);
        }

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

            ulong xorWithPrevious = (ulong)(prev.PreviousValue ^ val);

            prev.PreviousValue = val;

            prev.Count++;
            prev.Sum += dblVal;
            prev.Min = Math.Min(prev.Min, dblVal);
            prev.Max = Math.Max(prev.Max, dblVal);

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
        }

        public Enumerator GetEnumerator(ByteStringContext allocator) => new Enumerator(this, allocator);

        public struct TagPointer
        {
            public byte* Pointer;
            public int Length;

            public Span<byte> AsSpan()
            {
                return new Span<byte>(Pointer, Length);
            }
        }

        public struct Enumerator : IDisposable
        {
            private readonly TimeSeriesValuesSegment _parent;
            private int _bitsPosisition;
            private int _previousTimeStamp, _previousTimeStampDelta;
            private BitsBuffer _bitsBuffer;
            private ByteStringContext.InternalScope _scope;

            public Enumerator(TimeSeriesValuesSegment parent, ByteStringContext allocator)
            {
                _parent = parent;
                _bitsPosisition = 0;
                _previousTimeStamp = _previousTimeStampDelta = -1;
                _bitsBuffer = _parent.GetBitsBuffer(_parent.Header);
                if (_bitsBuffer.IsCompressed)
                {
                    _scope = _bitsBuffer.Uncompress(allocator, out _bitsBuffer);
                }
                else
                {
                    _scope = default;
                }
            }

            public bool MoveNext(out int timestamp, Span<double> values, Span<TimeStampState> state, ref TagPointer tag)
            {
                if (values.Length != _parent.Header->NumberOfValues)
                    ThrowInvalidNumberOfValues();


                if (_bitsPosisition >= _bitsBuffer.NumberOfBits)
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

                timestamp = ReadTimeStamp(_bitsBuffer);

                ReadValues(MemoryMarshal.Cast<double, long>(values), state);

                var differentTag = _bitsBuffer.ReadValue(ref _bitsPosisition, 1);
                if (differentTag == 1)
                {
                    var hasTag = _bitsBuffer.ReadValue(ref _bitsPosisition, 1);
                    if(hasTag == 0)
                    {
                        tag = default;
                    }
                    ReadTagValueByIndex(ref tag);
                }

                return true;
            }

            private void ReadTagValueByIndex(ref TagPointer tag)
            {
                var tagIndex = (int)_bitsBuffer.ReadValue(ref _bitsPosisition, 8);
                var offset = _parent.Header->SizeOfTags;
                var tagsPtr = _parent._buffer + sizeof(SegmentHeader) + sizeof(StatefulTimeStampValue) * _parent.Header->NumberOfValues;
                var numberOfTags = *(tagsPtr + offset - 1);
                if (tagIndex < 0 || tagIndex >= numberOfTags)
                    throw new ArgumentOutOfRangeException("Tag index " + tagIndex + " is outside the boundaries for tags for this segment: 0.." + numberOfTags);
                var tagsLens = tagsPtr + offset - 1 - numberOfTags;
                var tagBackwardOffset = 0;
                for (int i = numberOfTags - 1; i >= tagIndex; i--)
                {
                    tagBackwardOffset += tagsLens[i];
                }
                tag = new TagPointer
                {
                    Length = tagsLens[tagIndex],
                    Pointer = tagsLens - tagBackwardOffset
                };
            }

            private void ReadValues(Span<long> values, Span<TimeStampState> state)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    var nonZero = _bitsBuffer.ReadValue(ref _bitsPosisition, 1);
                    if (nonZero == 0)
                    {
                        continue; // no change since last time
                    }
                    var usePreviousBlockInfo = _bitsBuffer.ReadValue(ref _bitsPosisition, 1);
                    long xorValue;

                    if (usePreviousBlockInfo == 1)
                    {
                        xorValue = (long)_bitsBuffer.ReadValue(ref _bitsPosisition, 64 - state[i].LeadingZeroes - state[i].TrailingZeroes);
                        xorValue <<= state[i].TrailingZeroes;
                    }
                    else
                    {
                        int leadingZeros = (int)_bitsBuffer.ReadValue(ref _bitsPosisition, LeadingZerosLengthBits);
                        int blockSize = (int)_bitsBuffer.ReadValue(ref _bitsPosisition, BlockSizeLengthBits) + BlockSizeAdjustment;
                        int trailingZeros = 64 - blockSize - leadingZeros;

                        xorValue = (long)_bitsBuffer.ReadValue(ref _bitsPosisition, blockSize);
                        xorValue <<= trailingZeros;

                        state[i].TrailingZeroes = (byte)trailingZeros;
                        state[i].LeadingZeroes = (byte)leadingZeros;
                    }
                    values[i] = values[i] ^ xorValue;                }
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

            public void Dispose()
            {
                _scope.Dispose();
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
