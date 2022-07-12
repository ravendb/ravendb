using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Voron.Impl;
using Voron.Impl.Paging;
using Constants = Voron.Global.Constants;

namespace Voron.Data.Sets
{
    public readonly unsafe struct SetLeafPage
    {
        private readonly Page _page;
        private const int MaxNumberOfRawValues = 256;
        private const int MinNumberOfRawValues = 64;
        private const int MaxNumberOfCompressedEntries = 16;
        public SetLeafPageHeader* Header => (SetLeafPageHeader*)_page.Pointer;       

        public struct CompressedHeader
        {
            public ushort Position;
            public ushort Length;

            public override string ToString()
            {
                return "Pos: " + Position + ", Len: " + Length; 
            }
        }

        public readonly Span<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return new Span<byte>(_page.Pointer, Constants.Storage.PageSize); }
        }

        public readonly byte* Ptr
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _page.Pointer; }
        }

        public Span<CompressedHeader> Positions
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return new Span<CompressedHeader>(_page.Pointer + PageHeader.SizeOf, Header->NumberOfCompressedPositions); }
        }

        public CompressedHeader* PositionsPtr
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (CompressedHeader*)(_page.Pointer + PageHeader.SizeOf); }
        }

        private int OffsetOfRawValuesStart
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Constants.Storage.PageSize - (Header->NumberOfRawValues * sizeof(int)); }
        }
        
        private Span<int> RawValues
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return new Span<int>(_page.Pointer + OffsetOfRawValuesStart, Header->NumberOfRawValues); }
        }

        public SetLeafPage(Page page)
        {
            _page = page;
        }

        public void Init(long baseline)
        {
            Header->Baseline = baseline & ~int.MaxValue;
            Header->Flags = PageFlags.Single | PageFlags.Other;
            Header->SetFlags = ExtendedPageType.SetLeaf;
            Header->CompressedValuesCeiling = (ushort)(PageHeader.SizeOf + MaxNumberOfCompressedEntries  * sizeof(CompressedHeader));
            Header->NumberOfCompressedPositions = 0;
            Header->NumberOfRawValues = 0;
        }

        [SkipLocalsInit]
        public struct Iterator 
        {
            private SetLeafPage _parent;
            private const int MoveNextBufferSize = 32;
            private int _moveNextIndex, _moveNextLength;
            private int _compressIndex, _compressLength;
            private CompressedHeader _compressedEntry;
            private long _lastVal;
            private int _rawValuesIndex, _compressedEntryIndex;
            private PForDecoder.DecoderState _decoderState;
            private bool _hasDecoder;

            private fixed int _pforBuffer[PForEncoder.BufferLen];
            private fixed long _moveNextBuffer[MoveNextBufferSize];

            public bool IsInRange(long v)
            {
                if(_rawValuesIndex >= _parent.RawValues.Length  || _rawValuesIndex < 0)
                {
                    _rawValuesIndex = _parent.RawValues.Length-1; // just reset it, cheap to scan
                }
                if(_rawValuesIndex >= 0)// maybe we have no raw values?
                {
                    if (v <= (_parent.RawValues[_rawValuesIndex] & ~int.MaxValue))
                        return false; // need to get v as the next call
                }
                if (v <= _lastVal)// we expect to get v on the _next_ MoveNext()
                    return false;

                if (_compressedEntryIndex >= _parent.Header->NumberOfCompressedPositions)
                    return false;

                var end = _parent.Header->Baseline | (long)_parent.GetCompressRangeEnd(ref _parent.Positions[_compressedEntryIndex]);
                return  v <= end;
            }

            public Iterator(SetLeafPage parent)
            {
                _parent = parent;
                _rawValuesIndex = _parent.Header->NumberOfRawValues-1;
                _compressedEntryIndex = 0;                
                _hasDecoder = parent.Header->NumberOfCompressedPositions > 0;

                _compressIndex = _compressLength = 0;
                _decoderState = default;
                _compressedEntry = default;
                _lastVal = long.MinValue;
                _moveNextIndex = 0;
                _moveNextLength = 0;

                if (_hasDecoder)
                    InitializeDecoder(0);
            }

            private void InitializeDecoder(int index)
            {
                _compressedEntry = _parent.Positions[index];
                _decoderState = PForDecoder.Initialize(_parent.Span.Slice(_compressedEntry.Position, _compressedEntry.Length));
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext(out long l)
            {
                if (_moveNextIndex < _moveNextLength)
                {
                    l = _moveNextBuffer[_moveNextIndex++];
                    return true;
                }

                fixed (long* pBuf = _moveNextBuffer)
                {
                    Fill(new Span<long>(pBuf, MoveNextBufferSize), out _moveNextLength, out bool _);
                    var hasResult =_moveNextLength > 0;
                    l = hasResult ? _moveNextBuffer[0] : -1; 
                    _moveNextIndex = 1; // we already consumed the first item
                    return hasResult;
                }
            }

            public bool Skip(long val)
            {
                var iVal = (int)(val & int.MaxValue);
                _rawValuesIndex = _parent.RawValues.BinarySearch(iVal, new CompareIntsWithoutSignDescending());
                if (_rawValuesIndex < 0)
                    _rawValuesIndex = ~_rawValuesIndex -1;
                SkipToCompressedEntryFor(iVal, int.MaxValue);

                fixed (long* pBuf = _moveNextBuffer)
                {
                    while (true)
                    {
                        Fill(new Span<long>(pBuf, MoveNextBufferSize), out _moveNextLength, out bool _);
                        if (_moveNextLength == 0)
                            return false;
                        if (val > pBuf[_moveNextLength - 1])
                            continue;
                        _moveNextIndex = new Span<long>(pBuf, _moveNextLength).BinarySearch(val);
                        if (_moveNextIndex < 0)
                            _moveNextIndex = ~_moveNextIndex;
                        return true;
                    }
                }
            }

            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Fill(Span<long> matches, out int numberOfMatches, out bool hasPrunedResults, long pruneGreaterThanOptimization = long.MaxValue)
            {
                var compressIndex = _compressIndex;
                var compressLength = _compressLength;
                var decoderState = (PForDecoder.DecoderState*)Unsafe.AsPointer(ref _decoderState);
                var parent = (SetLeafPage*)Unsafe.AsPointer(ref _parent);
                var parentRawValues = parent->RawValues;
                fixed (int* scratchPtr = _pforBuffer)
                {
                    var rawValuesIndex = _rawValuesIndex;

                    numberOfMatches = 0;
                    hasPrunedResults = false;
                    int matchesLength = matches.Length;
                    long baseline = _parent.Header->Baseline;
                    // There are two different ways we can get values, they either come "from the raw values" OR "from the compressed sections"
                    // We write the raw values in reverse order (that we guarantee through sorting) but they can become intermixed with the
                    // compressed values. Therefore, we have to take care of all the possible combinations. We can have only raw values,
                    // only compressed and a mixture of the two. And whatever we do, we are always required to return them in order. 

                    bool hasRawValue = false, hasCompressedValue = false;
                    int rawValue = -1; // sentinel value 
                    int compressedValue = -1; // sentinel value 
                    int rawValueMasked = int.MaxValue;
                    while (numberOfMatches < matchesLength)
                    {
                        if (rawValuesIndex >= 0 && hasRawValue == false)
                        {
                            rawValue = parentRawValues[rawValuesIndex];
                            rawValueMasked = rawValue & int.MaxValue;
                            hasRawValue = true;
                        }

                        if (hasCompressedValue == false && _hasDecoder)
                        {
                            if (compressIndex >= compressLength)
                            {
                                TryReadMoreCompressedValues(parent, decoderState, ref _compressedEntry, ref compressIndex, ref compressLength,
                                    ref _compressedEntryIndex, ref _hasDecoder, scratchPtr, PForEncoder.BufferLen);
                            }

                            // We haven't got any compressed value yet, so we are getting it from the scratch pad.
                            if (compressIndex < compressLength)
                            {
                                compressedValue = scratchPtr[compressIndex++];
                                hasCompressedValue = true;
                            }
                        }

                        if (hasCompressedValue == false && hasRawValue == false)
                            break; // nothing more to read...
                        
                        long value;
                        if (rawValueMasked <= compressedValue || hasCompressedValue == false)
                        {
                            rawValuesIndex--; // increase to the _next_ highest value, since we are sorted in descending order
                            value = rawValue;

                            hasRawValue = false;
                            rawValueMasked = int.MaxValue;

                            if (compressedValue == (value & int.MaxValue))
                                hasCompressedValue = false;
                            
                            // This is a removed value, so we remove it. 
                            if (value < 0)
                            {
                                // It is a removal of an existing compressed value, then we signal that we have consumed the compressed value too. 
                                continue;
                            }

                            value &= int.MaxValue;
                        }
                        else // we have a raw value, but it is bigger than the current compressed value, therefore we need to read the compressed value
                        {
                            value = compressedValue;
                            hasCompressedValue = false;
                        }

                        // The value is actually signaling that we are done for this range.                        
                        value |= baseline;
                        matches[numberOfMatches++] = value;
                        
                        // we need to send the value to the user before we stop the interation
                        if (value > pruneGreaterThanOptimization)
                        {
                            hasPrunedResults = true; // We are pruning, we are done
                            break;
                        }

                    }

                    if (hasCompressedValue)
                        compressIndex--; // so next call will start here...

                    _rawValuesIndex = rawValuesIndex;
                    _compressIndex = compressIndex;
                    _compressLength = compressLength;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static void TryReadMoreCompressedValues(SetLeafPage* parent, 
                PForDecoder.DecoderState* decoderState, 
                ref CompressedHeader compressedEntry, 
                ref int compressIndex,
                ref int compressLength,
                ref int compressedEntryIndex,
                ref bool hasDecoder,
                int* scratch, 
                int scratchSize)
            {
                var parentPtr = parent->Ptr;
                var parentHeader = parent->Header;
                var parentPositions = parent->PositionsPtr;

                while (compressIndex == compressLength && hasDecoder)
                {
                    compressIndex = 0;
                    compressLength = PForDecoder.Decode(decoderState, parentPtr + compressedEntry.Position, compressedEntry.Length, scratch, scratchSize);
                    if (compressLength != 0)
                        return;

                    if (++compressedEntryIndex >= parentHeader->NumberOfCompressedPositions)
                    {
                        hasDecoder = false;
                        return;
                    }

                    compressedEntry = parentPositions[compressedEntryIndex];
                    PForDecoder.Reset(decoderState, compressedEntry.Length);                    
                }
            }


            public int CompressedEntryIndex => _compressedEntryIndex;

            internal void SkipToCompressedEntryFor(int value, int sizeLimit)
            {
                _compressedEntryIndex = 0;
                for (; _compressedEntryIndex < _parent.Header->NumberOfCompressedPositions; _compressedEntryIndex++)
                {
                    var end = _parent.GetCompressRangeEnd(ref _parent.Positions[_compressedEntryIndex]);
                    if (end >= value || _parent.Positions[_compressedEntryIndex].Length > sizeLimit)
                        break;
                }
                if (_compressedEntryIndex < _parent.Header->NumberOfCompressedPositions)
                {
                    InitializeDecoder(_compressedEntryIndex);
                    _hasDecoder = true;
                }
                else
                {
                    _hasDecoder = false;
                    _decoderState = default;
                }
            }

            public int TryFill(Span<long> matches, long pruneGreaterThanOptimization)
            {
                if (_moveNextIndex >= _moveNextLength)
                    return 0;
                
                var copy = Math.Min(matches.Length, _moveNextLength);
                var start = _moveNextIndex; 
                for (; _moveNextIndex < copy; _moveNextIndex++)
                {
                    long match = _moveNextBuffer[_moveNextIndex];
                    if (match > pruneGreaterThanOptimization)
                        break;
                    matches[_moveNextIndex - start] = match;
                }
                return _moveNextIndex - start;
            }
        }

        public List<long> GetDebugOutput(LowLevelTransaction llt)
        {
            var list = new List<long>();
            var it = GetIterator(llt);
            while (it.MoveNext(out long cur))
            {
                list.Add(cur);
            }
            return list;
        }

        public Iterator GetIterator(LowLevelTransaction llt) => new Iterator(this);

        public bool Add(LowLevelTransaction tx, long value)
        {
            Debug.Assert(IsValidValue(value));
            return AddInternal(tx, (int)value & int.MaxValue);
        }

        public bool IsValidValue(long value)
        {
            return (value & ~int.MaxValue) == Header->Baseline;
        }

        public bool Remove(LowLevelTransaction tx, long value)
        {
            Debug.Assert((value & ~int.MaxValue) == Header->Baseline);
            return AddInternal(tx, int.MinValue | ((int)value & int.MaxValue));
        }

        private bool AddInternal(LowLevelTransaction tx, int value)
        {
            var oldRawValues = RawValues;

            var index = oldRawValues.BinarySearch(value,
                // using descending values to ensure that adding new values in order
                // will do a minimum number of memcpy()s
                new CompareIntsWithoutSignDescending());
            if (index >= 0)
            {
                // overwrite it (maybe add on removed value).
                oldRawValues[index] = value;
                return true;
            }

            // need to add a value, let's check if we can...
            if (Header->NumberOfRawValues == MaxNumberOfRawValues || // the raw values range is full
                RunOutOfFreeSpace) // run into the compressed, cannot proceed
            {
                using var cmp = new Compressor(this, tx);
                if (cmp.TryCompressRawValues() == false)
                    return false;

                // we didn't free enough space
                if (Header->CompressedValuesCeiling > Constants.Storage.PageSize - MinNumberOfRawValues * sizeof(int))
                    return false;
                return AddInternal(tx, value);
            }

            Header->NumberOfRawValues++; // increase the size of the buffer _downward_
            var newRawValues = RawValues;
            index = ~index;
            oldRawValues.Slice(0, index).CopyTo(newRawValues);
            newRawValues[index] = value;
            return true;
        }

        private bool RunOutOfFreeSpace => Header->CompressedValuesCeiling > OffsetOfRawValuesStart - sizeof(int);

        public int SpaceUsed
        {
            get
            {
                var positions = Positions;
                var size = RawValues.Length * sizeof(int) + positions.Length * sizeof(CompressedHeader);
                for (int i = 0; i < positions.Length; i++)
                {
                    size += positions[i].Length;
                }
                return size;
            }
        }

        private int GetCompressRangeEnd(ref CompressedHeader pos)
        {
            var compressed = new Span<byte>(_page.Pointer + pos.Position, pos.Length);
            var end = MemoryMarshal.Cast<byte, int>(compressed.Slice(compressed.Length -4))[0];
            return end;
        }

        private ref struct Compressor
        {
            private readonly LowLevelTransaction _llt;
            private readonly SetLeafPage _parent;
            private readonly TemporaryPage _tmpPage;
            private readonly IDisposable _releaseTempPage;
            private readonly Span<byte> _output;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseOutput;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseScratch;
            private readonly Span<uint> _scratchEncoder;
            private readonly Span<int> _scratchDecoder;
            private readonly Span<int> _rawValues;
            private readonly ref SetLeafPageHeader TempHeader => ref MemoryMarshal.AsRef<SetLeafPageHeader>(_tmpPage.AsSpan());
            private readonly Span<CompressedHeader> _tempPositions;

            public void Dispose()
            {
                _releaseTempPage.Dispose();
                _releaseOutput.Dispose();
                _releaseScratch.Dispose();
            }

            public Compressor(SetLeafPage parent, LowLevelTransaction tx)
            {
                _llt = tx;
                _parent = parent;
                _releaseTempPage = tx.Environment.GetTemporaryPage(tx, out _tmpPage);
                _tmpPage.AsSpan().Clear();
                // we allocate 2 KB in size here, but we stop compression at ~1KB or so
                _releaseOutput = tx.Allocator.Allocate(MaxNumberOfRawValues * sizeof(int) * 2, out _output);
                _output.Clear();
                _releaseScratch = tx.Allocator.Allocate(PForEncoder.BufferLen*2, out Span<int> scratch);
                _scratchDecoder = scratch.Slice(PForEncoder.BufferLen);
                _scratchEncoder = MemoryMarshal.Cast<int, uint>(scratch.Slice(0, PForEncoder.BufferLen));
                _rawValues = _parent.RawValues;
                _parent.Span.Slice(0, PageHeader.SizeOf).CopyTo(_tmpPage.AsSpan());
                _tempPositions = new Span<CompressedHeader>(_tmpPage.TempPagePointer + PageHeader.SizeOf, MaxNumberOfCompressedEntries);
                
                TempHeader.PageNumber = _parent.Header->PageNumber;
                TempHeader.CompressedValuesCeiling = (ushort)(PageHeader.SizeOf + MaxNumberOfCompressedEntries * sizeof(CompressedHeader));
                TempHeader.NumberOfRawValues = 0;
            }

            public bool TryCompressRawValues()
            {
                int compressedEntryIndex = 0;
                var it = _parent.GetIterator(_llt);

                if (_parent.Header->NumberOfCompressedPositions != MaxNumberOfCompressedEntries &&
                    _parent.Header->NumberOfRawValues != 0)
                {
                    var sizeConstrained = (Constants.Storage.PageSize - _parent.Header->CompressedValuesCeiling) < 1024;
                    // optimize the compaction by merging just the relevant values
                    it.SkipToCompressedEntryFor(_rawValues[^1] & int.MaxValue,
                        // This determine when we should recompress the whole page to save more space
                        sizeConstrained ? _output.Length / 2 : int.MaxValue);
                    if(it.CompressedEntryIndex != 0) // we can skip some values, so let's do that
                    {
                        compressedEntryIndex = it.CompressedEntryIndex;
                        if (TryCopyPreviousCompressedEntries(compressedEntryIndex) == false)
                            return false;
                    }
                }

                var maxBits = _output.Length * 8 / 2; // we allocated 2KB, but we stopped at roughly the 1KB marker
                var encoder = new PForEncoder(_output, _scratchEncoder);
                while (it.MoveNext(out long lv) )
                {
                    var v = (int)lv & int.MaxValue;
                    if (encoder.TryAdd(v) == false)
                        return false;
                    if (encoder.ConsumedBits < maxBits) 
                        continue;
                    
                    if (compressedEntryIndex >= MaxNumberOfCompressedEntries || 
                        encoder.TryClose() == false || 
                        TryWriteCompressedEntryAt(_output.Slice(0, encoder.SizeInBytes), compressedEntryIndex++) == false)
                        return false;
                    encoder = new PForEncoder(_output, _scratchEncoder);
                }

                if (encoder.NumberOfAdditions > 0)
                {
                    if (compressedEntryIndex >= MaxNumberOfCompressedEntries)
                        return false;
                    if (encoder.TryClose() == false ||
                        TryWriteCompressedEntryAt(_output.Slice(0, encoder.SizeInBytes), compressedEntryIndex++) == false)
                        return false;
                }
                TempHeader.NumberOfCompressedPositions = (byte)compressedEntryIndex;
                // successful, so can copy back
                _tmpPage.AsSpan().CopyTo(_parent.Span);
                return true;
            }

            private readonly bool TryCopyPreviousCompressedEntries(int compressedEntryIndex)
            {
                for (int i = 0; i < compressedEntryIndex; i++)
                {
                    Span<byte> buffer = _parent.Span.Slice(_parent.Positions[i].Position, _parent.Positions[i].Length);
                    if (TryWriteCompressedEntryAt(buffer, i) == false)
                        return false;
                }
                return true;
            }

            private readonly bool TryWriteCompressedEntryAt(in Span<byte> buffer, int index)
            {
                _tempPositions[index] = new CompressedHeader {Length = (ushort)buffer.Length, Position = TempHeader.CompressedValuesCeiling};
                if (buffer.Length + TempHeader.CompressedValuesCeiling > Constants.Storage.PageSize)
                    return false;
                buffer.CopyTo(_tmpPage.AsSpan().Slice(TempHeader.CompressedValuesCeiling));
                TempHeader.CompressedValuesCeiling += (ushort)buffer.Length;
                return true;
            }
        }

        public void SplitHalfInto(ref SetLeafPage newPage)
        {
            newPage.Init(Header->Baseline);

            for (int i = Header->NumberOfCompressedPositions / 2; i < Header->NumberOfCompressedPositions ; i++)
            {
                var newIndex = newPage.Header->NumberOfCompressedPositions++;

                newPage.Positions[newIndex] = new CompressedHeader
                {
                    Length = Positions[i].Length,
                    Position = newPage.Header->CompressedValuesCeiling
                };
                Span.Slice(Positions[i].Position, Positions[i].Length)
                    .CopyTo(newPage.Span.Slice(newPage.Header->CompressedValuesCeiling));
                newPage.Header->CompressedValuesCeiling += Positions[i].Length;
            }
            Header->NumberOfCompressedPositions /= 2; // truncate current positions
            Debug.Assert(Header->NumberOfCompressedPositions > 0);
            var nextCompressedValue = GetCompressRangeEnd(ref Positions[^1]) + 1;
            var index = RawValues.BinarySearch(nextCompressedValue, new CompareIntsWithoutSignDescending());
            if (index < 0)
                index = ~index;
            newPage.Header->NumberOfRawValues = (ushort)(Header->NumberOfRawValues - index);
            Debug.Assert(newPage.OffsetOfRawValuesStart > newPage.Header->CompressedValuesCeiling);
            RawValues.Slice(index).CopyTo(newPage.RawValues);
            Header->NumberOfRawValues = (ushort)index;
        }

        private struct CompareIntsWithoutSignDescending : IComparer<int>
        {
            public int Compare(int x, int y)
            {
                x &= int.MaxValue;
                y &= int.MaxValue;
                return y - x;
            }
        }

        public (long First, long Last) GetRange()
        {
            int? first = null;
            int? last = null;
            Debug.Assert(Header->NumberOfCompressedPositions > 0 || Header->NumberOfRawValues > 0);

            if (Header->NumberOfCompressedPositions > 0)
            {
                ref var pos = ref Positions[^1];
                last = GetCompressRangeEnd(ref pos);

                pos = ref Positions[0];
                Span<int> scratch = stackalloc int[PForEncoder.BufferLen];
                var compressedEntryBuffer = Span.Slice(pos.Position, pos.Length);
                var decoderState = PForDecoder.Initialize(compressedEntryBuffer);
                var decoded = PForDecoder.Decode(ref decoderState, compressedEntryBuffer, scratch);
                Debug.Assert(decoded > 0);
                first = scratch[0];
            }

            var values = RawValues;
            if (values.IsEmpty == false)
            {
                int value = values[0];
                if (value < 0)
                    value &= ~int.MaxValue;
                if (last == null || last.Value < value)
                    last = value;

                value = values[^1];
                if (value < 0)
                    value &= ~int.MaxValue;
                if (first == null || first > value)
                    first = value;
            }

            Debug.Assert(first != null, nameof(first) + " != null");
            Debug.Assert(last != null, nameof(last) + " != null");
            return (Header->Baseline | (long)first.Value, Header->Baseline | (long)last.Value);
        }
    }
    
}
