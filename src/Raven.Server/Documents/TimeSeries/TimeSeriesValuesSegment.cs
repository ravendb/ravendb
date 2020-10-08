using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;

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

        public StatefulTimestampValueSpan SegmentValues =>
            new StatefulTimestampValueSpan(
                (StatefulTimestampValue*)(_buffer + sizeof(SegmentHeader)),
                Header->NumberOfValues
            );

        public ulong Hash => Hashing.XXHash64.Calculate(_buffer, (ulong)NumberOfBytes);

        public DateTime GetLastTimestamp(DateTime baseline)
        {
            return baseline.AddMilliseconds(Header->PreviousTimestamp);
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

            if (_capacity <= 0 || _capacity > 2048)
                InvalidCapacity();
        }

        public void CopyTo(byte* dest)
        {
            Memory.Copy(dest, _buffer, _capacity);
        }

        public AllocatedMemoryData Clone(JsonOperationContext context, out TimeSeriesValuesSegment segment)
        {
            // for better reuse let use the const size of 'MaxSegmentSize'
            var memory = context.GetMemory(TimeSeriesStorage.MaxSegmentSize);
            Memory.Set(memory.Address, 0, TimeSeriesStorage.MaxSegmentSize);
            CopyTo(memory.Address);
            segment = new TimeSeriesValuesSegment(memory.Address, _capacity);
            return memory;
        }

        public TimeSeriesSegmentSummary GetSummary()
        {
            if (NumberOfLiveEntries == 0)
                return default;

            return new TimeSeriesSegmentSummary(this);
        }

        private void InvalidCapacity()
        {
            throw new ArgumentOutOfRangeException("Maximum capacity for segment is 2KB, but was: " + _capacity);
        }

        private static int GetDataStart(SegmentHeader* header) => sizeof(SegmentHeader) + sizeof(StatefulTimestampValue) * header->NumberOfValues + header->SizeOfTags;

        public int NumberOfValues => Header->NumberOfValues;

        public int NumberOfEntries => Header->NumberOfEntries;
        public SegmentVersion Version => Header->Version;

        public int NumberOfLiveEntries => SegmentValues.NumberOfEntries;

        public void Initialize(int numberOfValues)
        {
            new Span<byte>(_buffer, _capacity).Clear();
            if (numberOfValues > MaxNumberOfValues)
                ThrowValuesOutOfRange(numberOfValues);
            if (_capacity > ushort.MaxValue)
                ThrowInvalidCapacityLength();

            Header->NumberOfValues = (byte)numberOfValues;
            Header->SizeOfTags = 1;
            Header->PreviousTagIndex = byte.MaxValue;// invalid tag value
            Header->Version = SegmentVersion.Current;
        }

        public bool Append(ByteStringContext allocator, int deltaFromStart, double val, Span<byte> tag, ulong status)
        {
            return Append(allocator, deltaFromStart, new Span<double>(&val, 1), tag, status);
        }

        public const ulong Live = 0;
        public const ulong Dead = 1;
        public const int MaxNumberOfValues = 32;

        public static void AssertValueStatus(ulong status)
        {
            if (status != Live && status != Dead)
                throw new InvalidOperationException("Schrödinger's cat must be either 'Dead' or 'Alive'.");
        }

        public bool Append(ByteStringContext allocator, int deltaFromStart, Span<double> vals, Span<byte> tag, ulong status)
        {
            if (vals.Length != Header->NumberOfValues)
                ThrowInvalidNumberOfValues(vals);
            if (tag.Length > byte.MaxValue)
                ThrowInvalidTagLength();
            if (deltaFromStart < 0)
                ThrowInvalidDelta();

            if (Header->NumberOfEntries == ushort.MaxValue)
                return false;

            var maximumSize =
                    sizeof(BitsBufferHeader) +
                    sizeof(int) + // max timestamp
                    1 + // timestamp uses ControlValue which takes 4 bits, need to account for this
                    sizeof(double) * vals.Length + vals.Length /* may use additional 2 bits per value here for the first items */ +
                    ((LeadingZerosLengthBits + BlockSizeLengthBits) / 8 + 1) * vals.Length + // LeadingZerosLengthBits and BlockSizeLengthBits for each value (total 11 bits)
                    2 + // previous tag position (10 bits)
                    1 // take into account the status bit
                ;

            var copiedHeaderSize = sizeof(SegmentHeader) + Header->NumberOfValues * sizeof(StatefulTimestampValue);
            using (allocator.Allocate(maximumSize + copiedHeaderSize, out var tempBuffer))
            {
                Memory.Set(tempBuffer.Ptr, 0, maximumSize);

                var tempHeader = (SegmentHeader*)(tempBuffer.Ptr + maximumSize);

                Memory.Copy(tempHeader, Header, copiedHeaderSize);

                var tempBitsBuffer = new BitsBuffer(tempBuffer.Ptr, maximumSize);

                AssertValueStatus(status);
                tempBitsBuffer.AddValue(status, 1);

                var prevs = new Span<StatefulTimestampValue>((tempBuffer.Ptr + maximumSize) + sizeof(SegmentHeader), tempHeader->NumberOfValues);
                AddTimestamp(deltaFromStart, ref tempBitsBuffer, tempHeader);

                if (NumberOfLiveEntries == 0 &&
                    status == Live)
                {
                    for (int i = 0; i < vals.Length; i++)
                    {
                        prevs[i].Min = prevs[i].First = vals[i];
                    }
                }

                for (int i = 0; i < vals.Length; i++)
                {
                    AddValue(ref prevs[i], ref tempBitsBuffer, vals[i], status);
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
                        if (prevTagIndex == byte.MaxValue)
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

                    tempBitsBuffer.AddValue(3, 2); // will write the tag index buffer here
                    tempBitsBuffer.AddValue((ulong)(~prevTagIndex), 7);
                    tempHeader->PreviousTagIndex = (byte)(~prevTagIndex);
                }

                tempHeader->PreviousTimestamp = deltaFromStart;
                tempHeader->NumberOfEntries++;
                var actualBitsBuffer = GetBitsBuffer(tempHeader);

                if (insertTag)
                {
                    if (actualBitsBuffer.EnsureAdditionalBits(allocator, tempBitsBuffer.NumberOfBits + (tag.Length + 1) * 8) == false)
                        return false;

                    if (InsertTag(tag, tempHeader, actualBitsBuffer.NumberOfBytes) == false)
                        return false;

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

                Memory.Copy(Header, tempHeader, copiedHeaderSize);

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
            var tagsPtr = _buffer + sizeof(SegmentHeader) + sizeof(StatefulTimestampValue) * tempHeader->NumberOfValues;
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

        public bool InsertTag(Span<byte> tag, SegmentHeader* tempHeader, int numberOfBytes)
        {
            var offset = tempHeader->SizeOfTags;
            var tagsPtr = _buffer + sizeof(SegmentHeader) + sizeof(StatefulTimestampValue) * tempHeader->NumberOfValues;
            var numberOfTags = tagsPtr[offset - 1];

            if (numberOfTags >= 127)
                return false;

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
            return true;
        }

        private static void ThrowInvalidTagLength()
        {
            throw new ArgumentOutOfRangeException("TimeSeries tag value cannot exceed 256 bytes");
        }

        private static void ThrowInvalidDelta()
        {
            throw new ArgumentOutOfRangeException("Delta can't be negative");
        }

        private static void ThrowInvalidFirstDelta()
        {
            throw new ArgumentOutOfRangeException("First value must be with zero delta");
        }

        private static void ThrowInvalidNewDelta()
        {
            throw new ArgumentOutOfRangeException("New timestamp must be greater then the previous");
        }

        [Pure]
        public BitsBuffer GetBitsBuffer(SegmentHeader* header)
        {
            int dataStart = GetDataStart(header);
            return new BitsBuffer(_buffer + dataStart, _capacity - dataStart);
        }

        private static void AddTimestamp(int deltaFromStart, ref BitsBuffer bitsBuffer, SegmentHeader* tempHeader)
        {
            if (tempHeader->NumberOfEntries == 0)
            {
                if (deltaFromStart != 0)
                    ThrowInvalidFirstDelta();

                bitsBuffer.AddValue((ulong)deltaFromStart, BitsForFirstTimestamp);
                tempHeader->PreviousDelta = DefaultDelta;
                return;
            }

            int delta = deltaFromStart - tempHeader->PreviousTimestamp;
            if (delta <= 0)
                ThrowInvalidNewDelta();

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
            tempHeader->PreviousDelta = delta;
        }


        private static void AddValue(ref StatefulTimestampValue prev, ref BitsBuffer bitsBuffer, double dblVal, ulong status)
        {
            long val = BitConverter.DoubleToInt64Bits(dblVal);

            ulong xorWithPrevious = (ulong)(prev.PreviousValue ^ val);

            if (IsValid(status, val))
            {
                if (double.IsNaN(prev.First))
                    prev.First = dblVal;

                prev.PreviousValue = val;

                prev.Count++;
                prev.Sum += dblVal;
                prev.Max = double.IsNaN(prev.Max)
                    ? dblVal
                    : Math.Max(prev.Max, dblVal);
                prev.Min = double.IsNaN(prev.Min)
                    ? dblVal
                    : Math.Min(prev.Min, dblVal);
            }

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

        public static LazyStringValue SetTimestampTag(JsonOperationContext context, TagPointer tagPointer)
        {
            if (tagPointer.Pointer == null)
                return null;

            return context.GetLazyStringValue(tagPointer.Pointer);
        }

        public IEnumerable<SingleResult> YieldAllValues(DocumentsOperationContext context, DateTime baseline, bool includeDead = true)
        {
            return YieldAllValues(context, context.Allocator, baseline, includeDead);
        }

        public IEnumerable<SingleResult> YieldAllValues(JsonOperationContext context, ByteStringContext allocator, DateTime baseline, bool includeDead = true)
        {
            var values = new double[NumberOfValues];
            var states = new TimestampState[NumberOfValues];
            DateTime current = default;

            var tagPointer = new TagPointer();
            using (var enumerator = GetEnumerator(allocator))
            {
                while (enumerator.MoveNext(out int ts, values, states, ref tagPointer, out var status))
                {
                    if (status == Dead && includeDead == false)
                        continue;

                    var next = baseline.AddMilliseconds(ts);
                    if (next == current && 
                        Version == SegmentVersion.V50000) // fix legacy issue RavenDB-15617
                        continue;

                    current = next;

                    var tag = SetTimestampTag(context, tagPointer);

                    var length = values.Length;
                    while (length > 0 && double.IsNaN(values[length - 1]))
                    {
                        length--;
                    }
                    
                    yield return new SingleResult
                    {
                        Timestamp = current,
                        Tag = tag,
                        Values = new Memory<double>(values, 0, length),
                        Status = status
                    };
                }
            }
        }

        private enum ParsingOrder
        {
            Id,
            Name,
            BaseLine
        }

        public static void ParseTimeSeriesKey(byte* ptr, int size, JsonOperationContext context, out LazyStringValue docId, out LazyStringValue name)
        {
            docId = null;
            name = null;

            var bytes = new Span<byte>(ptr, size);
            var order = ParsingOrder.Id;
            var next = -1;
            for (int i = 0; i < bytes.Length; i++)
            {
                var val = bytes[i];
                if (val != SpecialChars.RecordSeparator)
                    continue;

                switch (order)
                {
                    case ParsingOrder.Id:
                        docId = context.GetLazyString(ptr, i);
                        break;

                    case ParsingOrder.Name:
                        name = context.GetLazyString(ptr + next, i - next);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
                next = i + 1;
                order++;

                if (order == ParsingOrder.BaseLine)
                    break;
            }

            Debug.Assert(docId != null);
            Debug.Assert(name != null);
        }

        public static void ParseTimeSeriesKey(byte* ptr, int size, JsonOperationContext context, out LazyStringValue docId, out LazyStringValue name, out DateTime baseLine)
        {
            ParseTimeSeriesKey(ptr, size, context, out docId, out name);

            var bytes = new Span<byte>(ptr, size);
            var ticks = MemoryMarshal.Read<long>(bytes.Slice(bytes.Length - sizeof(long), sizeof(long)));
            baseLine = new DateTime(Bits.SwapBytes(ticks) * 10_000, DateTimeKind.Utc);
        }

        public static void ParseTimeSeriesKey(Slice key, JsonOperationContext context, out LazyStringValue docId, out LazyStringValue name)
        {
            ParseTimeSeriesKey(key.Content.Ptr, key.Size, context, out docId, out name);
        }

        public static void ParseTimeSeriesKey(Slice key, JsonOperationContext context, out LazyStringValue docId, out LazyStringValue name, out DateTime baseLine)
        {
            ParseTimeSeriesKey(key.Content.Ptr, key.Size, context, out docId, out name, out baseLine);
        }

        public struct TagPointer
        {
            public byte* Pointer;
            public int Length;

            public byte Size => *Pointer; // the first byte is the size

            public Span<byte> AsSpan()
            {
                if (Pointer == null || Size == 0)
                {
                    return Slices.Empty.AsSpan();
                }

                return new Span<byte>(Pointer, Length);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsValid(ulong status, long value)
        {
            // valid = live and not NaN
            return status == Live && (value & 0x7FFFFFFFFFFFFFFF) > 0x7FF0000000000000 == false;
        }

        public struct Enumerator : IDisposable
        {
            private readonly TimeSeriesValuesSegment _parent;
            private int _bitsPosition;
            private int _previousTimestamp, _previousTimestampDelta;
            private BitsBuffer _bitsBuffer;
            private ByteStringContext.InternalScope _scope;

            public Enumerator(TimeSeriesValuesSegment parent, ByteStringContext allocator)
            {
                _parent = parent;
                _bitsPosition = 0;
                _previousTimestamp = _previousTimestampDelta = -1;
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

            public bool MoveNext(out int timestamp, Span<double> values, Span<TimestampState> state, ref TagPointer tag, out ulong status)
            {
                if (values.Length != _parent.Header->NumberOfValues)
                    ThrowInvalidNumberOfValues();

                if (_bitsPosition >= _bitsBuffer.NumberOfBits)
                {
                    status = ulong.MaxValue; // invalid value
                    timestamp = default;
                    return false;
                }
                if (_bitsPosition == 0)
                {
                    // we use the values as the statement location for the previous values as well
                    values.Clear();
                    state.Clear();
                    tag = default;
                }

                status = _bitsBuffer.ReadValue(ref _bitsPosition, 1);

                timestamp = ReadTimestamp(_bitsBuffer);

                ReadValues(MemoryMarshal.Cast<double, long>(values), state, status);

                var differentTag = _bitsBuffer.ReadValue(ref _bitsPosition, 1);
                if (differentTag == 1)
                {
                    var hasTag = _bitsBuffer.ReadValue(ref _bitsPosition, 1);
                    if (hasTag == 0)
                    {
                        tag = default;
                    }
                    else
                    {
                        ReadTagValueByIndex(ref tag);
                    }
                }

                return true;
            }

            private void ReadTagValueByIndex(ref TagPointer tag)
            {
                var tagIndex = (int)_bitsBuffer.ReadValue(ref _bitsPosition, 7);
                var offset = _parent.Header->SizeOfTags;
                var tagsPtr = _parent._buffer + sizeof(SegmentHeader) + sizeof(StatefulTimestampValue) * _parent.Header->NumberOfValues;
                var numberOfTags = *(tagsPtr + offset - 1);
                if (tagIndex < 0 || tagIndex >= numberOfTags)
                    throw new ArgumentOutOfRangeException("Tag index " + tagIndex + " is outside the boundaries for tags for this segment: 0.." + numberOfTags);
                var tagsLens = tagsPtr + offset - 1 - numberOfTags;
                var tagBackwardOffset = 0;
                for (int i = numberOfTags - 1; i >= tagIndex; i--)
                {
                    tagBackwardOffset += tagsLens[i];
                }

                tag.Pointer = tagsLens - tagBackwardOffset;
                tag.Length = tagsLens[tagIndex];
                /*tag = new TagPointer
                {
                    Length = tagsLens[tagIndex],
                    Pointer = tagsLens - tagBackwardOffset
                };*/
            }

            private void ReadValues(Span<long> values, Span<TimestampState> state, ulong status)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    var nonZero = _bitsBuffer.ReadValue(ref _bitsPosition, 1);
                    if (nonZero == 0)
                    {
                        values[i] = state[i].LastValidValue;
                        continue; // no change since last time
                    }
                    var usePreviousBlockInfo = _bitsBuffer.ReadValue(ref _bitsPosition, 1);
                    long xorValue;

                    if (usePreviousBlockInfo == 1)
                    {
                        xorValue = (long)_bitsBuffer.ReadValue(ref _bitsPosition, 64 - state[i].LeadingZeroes - state[i].TrailingZeroes);
                        xorValue <<= state[i].TrailingZeroes;
                    }
                    else
                    {
                        int leadingZeros = (int)_bitsBuffer.ReadValue(ref _bitsPosition, LeadingZerosLengthBits);
                        int blockSize = (int)_bitsBuffer.ReadValue(ref _bitsPosition, BlockSizeLengthBits) + BlockSizeAdjustment;
                        int trailingZeros = 64 - blockSize - leadingZeros;

                        xorValue = (long)_bitsBuffer.ReadValue(ref _bitsPosition, blockSize);
                        xorValue <<= trailingZeros;

                        state[i].TrailingZeroes = (byte)trailingZeros;
                        state[i].LeadingZeroes = (byte)leadingZeros;
                    }

                    values[i] = state[i].LastValidValue ^ xorValue;

                    if (_parent.Version == SegmentVersion.V50000 || // backward comp.
                        IsValid(status, values[i]))
                    {
                        state[i].LastValidValue = values[i];
                    }
                }
            }

            private int ReadTimestamp(BitsBuffer bitsBuffer)
            {
                if (_bitsPosition == 1)
                {
                    _previousTimestamp = (int)bitsBuffer.ReadValue(ref _bitsPosition, BitsForFirstTimestamp);
                    _previousTimestampDelta = DefaultDelta;
                    return _previousTimestamp;
                }

                var type = bitsBuffer.FindTheFirstZeroBit(ref _bitsPosition, TimestampEncodingDetails.MaxControlBitLength);
                if (type > 0)
                {
                    var index = type - 1;
                    ref var encoding = ref TimestampEncodingDetails.Encodings[index];
                    long decodedValue = (long)bitsBuffer.ReadValue(ref _bitsPosition, encoding.BitsForValue);
                    // [0, 255] becomes [-128, 127]
                    decodedValue -= encoding.MaxValueForEncoding;
                    if (decodedValue >= 0)
                    {
                        // [-128, 127] becomes [-128, 128] without the zero in the middle
                        decodedValue++;
                    }
                    _previousTimestampDelta += (int)decodedValue;
                }
                _previousTimestamp += _previousTimestampDelta;

                return _previousTimestamp;
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

        // handle legacy issue RavenDB-15645
        // if we found that the 'Last' value of the 'StatefulTimestampValue' is NaN,
        // we will yield all values individually, instead of rely on it. 
        public bool InvalidLastValue()
        {
            if (Version != SegmentVersion.V50000)
                return false;

            if (NumberOfLiveEntries == 0)
                return false;

            foreach (var value in SegmentValues.Span)
            {
                if (double.IsNaN(value.Last))
                    return true;
            }

            return false;
        }
    }

    public readonly struct TimeSeriesSegmentSummary
    {
        public readonly IReadOnlyList<double> Min;

        public readonly IReadOnlyList<double> Max;

        public readonly IReadOnlyList<double> Sum;

        public readonly int Count;

        public TimeSeriesSegmentSummary(TimeSeriesValuesSegment segment)
        {
            Min = new InnerList(segment, SummaryType.Min);
            Max = new InnerList(segment, SummaryType.Max);
            Sum = new InnerList(segment, SummaryType.Sum);
            Count = segment.NumberOfLiveEntries;
        }

        private enum SummaryType
        {
            Min,
            Max,
            Sum
        }

        private class InnerList : IReadOnlyList<double>
        {
            private readonly TimeSeriesValuesSegment _segment;
            private readonly SummaryType _type;

            public InnerList(TimeSeriesValuesSegment segment, SummaryType type)
            {
                _segment = segment;
                _type = type;
            }

            double IReadOnlyList<double>.this[int index]
            {
                get
                {
                    if (index >= _segment.NumberOfValues)
                        throw new ArgumentOutOfRangeException(nameof(index));

                    return _type switch
                    {
                        SummaryType.Min => _segment.SegmentValues.Span[index].Min,
                        SummaryType.Max => _segment.SegmentValues.Span[index].Max,
                        SummaryType.Sum => _segment.SegmentValues.Span[index].Sum,
                        _ => throw new NotSupportedException($"Unknown summary type '{_type}'."),
                    };
                }
            }

            int IReadOnlyCollection<double>.Count => _segment.NumberOfValues;

            IEnumerator<double> IEnumerable<double>.GetEnumerator()
            {
                return GetEnumerable().GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerable().GetEnumerator();
            }

            private IEnumerable<double> GetEnumerable()
            {
                for (var index = 0; index < _segment.NumberOfValues; index++)
                {
                    yield return _type switch
                    {
                        SummaryType.Min => _segment.SegmentValues.Span[index].Min,
                        SummaryType.Max => _segment.SegmentValues.Span[index].Max,
                        SummaryType.Sum => _segment.SegmentValues.Span[index].Sum,
                        _ => throw new NotSupportedException($"Unknown summary type '{_type}'."),
                    };
                }
            }
        }
    }
}
