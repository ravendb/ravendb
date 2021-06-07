using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Voron.Impl;
using Voron.Impl.Paging;
using Constants = Voron.Global.Constants;

namespace Voron.Data.Sets
{
    public readonly unsafe struct SetLeafPage
    {
        private readonly byte* _base;
        private const int MaxNumberOfRawValues = 256;
        private const int MinNumberOfRawValues = 64;
        private const int MaxNumberOfCompressedEntries = 16;
        public SetLeafPageHeader* Header => ((SetLeafPageHeader*)_base);
        
        public Span<byte> Span => new Span<byte>(_base, Constants.Storage.PageSize);

        public struct CompressedHeader
        {
            public ushort Position;
            public ushort Length;

            public override string ToString()
            {
                return "Pos: " + Position + ", Len: " + Length; 
            }
        }

        public Span<CompressedHeader> Positions => new Span<CompressedHeader>(_base + PageHeader.SizeOf, Header->NumberOfCompressedPositions);
        private int OffsetOfRawValuesStart => Constants.Storage.PageSize - (Header->NumberOfRawValues * sizeof(int));
        private Span<int> RawValues => new Span<int>(_base + OffsetOfRawValuesStart, Header->NumberOfRawValues);
        
        public SetLeafPage(byte* @base)
        {
            _base = @base;
        }

        public void Init(long baseline)
        {
            Header->Baseline = baseline & ~int.MaxValue;
            Header->Flags = PageFlags.Single | PageFlags.SetPage;
            Header->SetFlags = SetPageFlags.Leaf;
            Header->CompressedValuesCeiling = (ushort)(PageHeader.SizeOf + MaxNumberOfCompressedEntries  * sizeof(CompressedHeader));
            Header->NumberOfCompressedPositions = 0;
            Header->NumberOfRawValues = 0;
        }

        public ref struct Iterator
        {
            private readonly SetLeafPage _parent;
            private readonly Span<int> _scratch;
            private Span<int> _current;
            private int _rawValuesIndex, _compressedEntryIndex;
            private PForDecoder _decoder;
            private bool _hasDecoder;

            public Iterator(SetLeafPage parent, Span<int> scratch)
            {
                _parent = parent;
                _scratch = scratch;
                _rawValuesIndex = _parent.Header->NumberOfRawValues-1;
                _compressedEntryIndex = 0;
                _current = default;
                _hasDecoder = parent.Header->NumberOfCompressedPositions > 0;
                _decoder = default;
                if (_hasDecoder)
                    InitializeDecoder(0);
            }

            private void InitializeDecoder(int index)
            {
                ref var pos = ref _parent.Positions[index];
                var compressedEntryBuffer = _parent.Span.Slice(pos.Position, pos.Length);
                _decoder = new PForDecoder(compressedEntryBuffer, _scratch);
            }

            public bool MoveNext(out long l)
            {
                var result = MoveNext(out int t);
                l = (long)t | _parent.Header->Baseline;
                return result;
            }

            public bool MoveNext(out int i)
            {
                TryReadMoreCompressedValues();

                while (_rawValuesIndex >= 0)
                {
                    // note, reading in reverse!
                    int rawValue = _parent.RawValues[_rawValuesIndex];
                    int rawValueMasked = rawValue & int.MaxValue;
                    if (_current.IsEmpty == false)
                    {
                        if(rawValueMasked > _current[0])
                            break; // need to read from the compressed first
                        if (rawValueMasked == _current[0])
                        {
                            _current = _current.Slice(1); // skip this one
                            TryReadMoreCompressedValues();
                        }
                    }
                    _rawValuesIndex--;
                    if (rawValue < 0) // removed, ignore
                        continue; 
                    i = rawValue;
                    return true;
                }

                if (_current.IsEmpty)
                {
                    i = default;
                    return false;
                }

                i = _current[0];
                _current = _current.Slice(1);
                return true;
            }

            private void TryReadMoreCompressedValues()
            {
                while (_current.IsEmpty && _hasDecoder)
                {
                    _current = _decoder.Decode();
                    if (_current.IsEmpty == false)
                        return;

                    if (++_compressedEntryIndex >= _parent.Header->NumberOfCompressedPositions)
                    {
                        _hasDecoder = false;
                        return;
                    }

                    ref var pos = ref _parent.Positions[_compressedEntryIndex];
                    var compressedEntryBuffer = _parent.Span.Slice(pos.Position, pos.Length);
                    _decoder = new PForDecoder(compressedEntryBuffer, _scratch);
                }
            }
            public void SkipTo(long val)
            {
                var iVal = (int)(val & int.MaxValue);
                _rawValuesIndex = _parent.RawValues.BinarySearch(iVal, new CompareIntsWithoutSignDescending());
                if (_rawValuesIndex < 0)
                    _rawValuesIndex = ~_rawValuesIndex - 1; // we are _after_ the value, so let's go back one step

                SkipToCompressedEntryFor(iVal, int.MaxValue);
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
                    _decoder = default;
                }
            }
        }

        public List<long> GetDebugOutput()
        {
            var list = new List<long>();
            Span<int> scratch = stackalloc int[128];
            var it = GetIterator(scratch);
            while (it.MoveNext(out long cur))
            {
                list.Add(cur);
            }
            return list;
        }

        public Iterator GetIterator(Span<int> scratch) => new Iterator(this, scratch);

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
                // overwrite it (maybe add on removed value.
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
            var compressed = new Span<byte>(_base + pos.Position, pos.Length);
            var end = MemoryMarshal.Cast<byte, int>(compressed.Slice(compressed.Length -4))[0];
            return end;
        }

        private ref struct Compressor
        {
            private readonly SetLeafPage _parent;
            private readonly TemporaryPage _tmpPage;
            private readonly IDisposable _releaseTempPage;
            private readonly Span<byte> _output;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseOutput;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseScratch;
            private readonly Span<uint> _scratchEncoder;
            private readonly Span<int> _scratchDecoder;
            private readonly Span<int> _rawValues;
            private readonly SetLeafPageHeader* _tempHeader;
            private readonly Span<CompressedHeader> _tempPositions;

            public void Dispose()
            {
                _releaseTempPage.Dispose();
                _releaseOutput.Dispose();
                _releaseScratch.Dispose();
            }

            public Compressor(SetLeafPage parent, LowLevelTransaction tx)
            {
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
                _tempHeader = (SetLeafPageHeader*)_tmpPage.TempPagePointer;
                _tempHeader->PageNumber = _parent.Header->PageNumber;
                _tempHeader->CompressedValuesCeiling = (ushort)(PageHeader.SizeOf + MaxNumberOfCompressedEntries * sizeof(CompressedHeader));
                _tempHeader->NumberOfRawValues = 0;
                _tempPositions = new Span<CompressedHeader>(_tmpPage.TempPagePointer + PageHeader.SizeOf, MaxNumberOfCompressedEntries);
            }

            private const int MaxPreferredEntrySize = (Constants.Storage.PageSize / MaxNumberOfCompressedEntries);

            public bool TryCompressRawValues()
            {
                var it = _parent.GetIterator(_scratchDecoder);
                int compressedEntryIndex = 0;
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
                while (it.MoveNext(out int v) )
                {
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
                _tempHeader->NumberOfCompressedPositions = (byte)compressedEntryIndex;
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
                _tempPositions[index] = new CompressedHeader {Length = (ushort)buffer.Length, Position = _tempHeader->CompressedValuesCeiling};
                if (buffer.Length + _tempHeader->CompressedValuesCeiling > Constants.Storage.PageSize)
                    return false;
                buffer.CopyTo(_tmpPage.AsSpan().Slice(_tempHeader->CompressedValuesCeiling));
                _tempHeader->CompressedValuesCeiling += (ushort)buffer.Length;
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
            int? first = null, last = null;
            Debug.Assert(Header->NumberOfCompressedPositions > 0 || Header->NumberOfRawValues > 0);

            if (Header->NumberOfCompressedPositions > 0)
            {
                ref var pos = ref Positions[^1];
                last = GetCompressRangeEnd(ref pos);

                pos = ref Positions[0];
                Span<int> scratch = stackalloc int[PForEncoder.BufferLen];
                var compressedEntryBuffer = Span.Slice(pos.Position, pos.Length);
                var decoder = new PForDecoder(compressedEntryBuffer, scratch);
                first = decoder.Decode()[0];
            }

            var values = RawValues;
            if (values.IsEmpty == false)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    if(values[i] < 0)
                        continue;
                    if (last == null || last.Value < values[i])
                        last = values[i];
                    break;
                }

                for (int i = values.Length - 1; i >= 0; i--)
                {
                    if(values[i] < 0)
                        continue;
                    if (first == null || first > values[i])
                        first = values[i];
                    break;
                }
            }

            Debug.Assert(first != null, nameof(first) + " != null");
            Debug.Assert(last != null, nameof(last) + " != null");
            return (Header->Baseline | (long)first.Value, Header->Baseline | (long)last.Value);
        }
    }
    
}
