using System;
using System.Diagnostics;
using System.Reflection.Metadata;
using System.Runtime.InteropServices;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.Sets
{
    public readonly unsafe struct SetBranchPage
    {
        private readonly byte* _base;

        public SetBranchPage(byte* baseline)
        {
            _base = baseline;
        }

        public long First => ZigZag.Decode(Span.Slice(Positions[0]));
        
        public long Last =>  ZigZag.Decode(Span.Slice(Positions[^1]));

        public void Init()
        {
            Header->Flags = PageFlags.Single | PageFlags.SetPage;
            Header->SetFlags = SetPageFlags.Branch;
            Header->NumberOfEntries = 0;
            Header->Upper = Constants.Storage.PageSize;
        }
        
        /// <summary>
        /// Even with no compression at all, we know that we can fit double that amount
        /// in a page. 8,192 bytes - 64 bytes header = 8,128
        /// We use ZigZag encoding, so max value of 10 bytes, so max value size is 20 (key & page)
        /// With 180 x 20 = 3,600 bytes + 360 bytes for the positions array
        /// So total of 3,960 bytes. Double that to 7,920 and we know that this is the guarantee merge factor
        /// </summary>
        public const int MinNumberOfValuesBeforeMerge = 180;
        
        public SetBranchPageHeader* Header => (SetBranchPageHeader*)_base;
        
        private Span<ushort> Positions => new Span<ushort>(_base + PageHeader.SizeOf, Header->NumberOfEntries);

        private int FreeSpace => Header->Upper - PageHeader.SizeOf - (Header->NumberOfEntries * sizeof(ushort));
        
        private Span<byte> Span => new Span<byte>(_base, Constants.Storage.PageSize);

        private void Defrag(LowLevelTransaction tx)
        {
            using var _ = tx.Environment.GetTemporaryPage(tx, out var tmp);
            Span<byte> tmpSpan = tmp.AsSpan();
            Span.CopyTo(tmpSpan);
            var header = (SetBranchPageHeader*)tmp.TempPagePointer;
            header->Upper = Constants.Storage.PageSize;
            var positions = MemoryMarshal.Cast<byte, ushort>(tmpSpan.Slice(PageHeader.SizeOf)).Slice(0, header->NumberOfEntries);
            int endOfPositionsArray = PageHeader.SizeOf + sizeof(ushort) * positions.Length;
            for (int i = 0; i < positions.Length; i++)
            {
                Span<byte> entry = Span.Slice(positions[i]);
                var len = ZigZag.SizeOfBoth(entry);

                header->Upper -= (ushort)len;
                Debug.Assert(header->Upper >= endOfPositionsArray);
                entry.Slice(0, len).CopyTo(tmpSpan.Slice(header->Upper));
                positions[i] = header->Upper;
            }
            // zero unused data
            tmpSpan.Slice(endOfPositionsArray, header->Upper - endOfPositionsArray).Clear();
            tmpSpan.CopyTo(Span);
        }

        public Iterator Iterate()
        {
            return new Iterator(this);
        }

        public struct Iterator
        {
            private readonly SetBranchPage _parent;
            private int _pos;

            public Iterator(SetBranchPage parent)
            {
                _parent = parent;
                _pos = 0;
            }

            public bool TryMoveNext(out long value)
            {
                if (_pos >= _parent.Header->NumberOfEntries)
                {
                    value = -1;
                    return false;
                }
                (_, value) = ZigZag.DecodeBoth(_parent.Span.Slice(_parent.Positions[_pos++]));
                return true;
            }
        }

        public (long Value, int Index, int Match) SearchPage(long key)
        {
            var (index, match) = SearchInPage(key);
            if (index < 0)
                index = ~index;
            if (match != 0)
                index--; // went too far
            var actual = Math.Min(Header->NumberOfEntries - 1, index);
            var (_, value) = ZigZag.DecodeBoth(Span.Slice(Positions[actual]));
            return (value, index, match);
        }

        public (long Key, long Page) GetByIndex(int index)
        {
            return ZigZag.DecodeBoth(Span.Slice(Positions[index]));
        }

        public bool TryGetValue(long key, out long value)
        {
            var (index, match) = SearchInPage(key);
            if (match != 0)
            {
                value = -1;
                return false;
            }
            (_, value) = ZigZag.DecodeBoth(Span.Slice(Positions[index]));
            return true;
        }

        public void Remove(long key)
        {
            var (index, match) = SearchInPage(key);
            if (match !=0 )
            {
                return;
            }
            Positions.Slice(index+1).CopyTo(Positions.Slice(index));
            Header->NumberOfEntries--;
        }
        
        public bool TryAdd(LowLevelTransaction tx, long key, long page)
        {
            Span<byte> buffer = stackalloc byte[24];
            var len = ZigZag.Encode(buffer, key);
            len += ZigZag.Encode(buffer.Slice(len), page);

            var (index, match) = SearchInPage(key);
            if (match ==  0)
            {
                var (_, existing) = ZigZag.DecodeBoth(Span.Slice(Positions[index]));
                if (existing == page)
                    return true; // already here
                
                // remove the existing value, we'll re-add it below
                Positions.Slice(index+1).CopyTo(Positions.Slice(index));
                Header->NumberOfEntries--;
            }
            else
            {
                index = ~index;
            }
            
            Debug.Assert(FreeSpace >= 0);
            if (len + sizeof(ushort) > FreeSpace)
            {
                Defrag(tx);
                if (len + sizeof(ushort) > FreeSpace)
                    return false;
            }

            // make room in array
            var old = Positions.Slice(index);
            Header->NumberOfEntries++;
            old.CopyTo(Positions.Slice(index + 1));
            
            Header->Upper -= (ushort)len;
            buffer.Slice(0, len).CopyTo(Span.Slice(Header->Upper));
            Positions[index] = Header->Upper;
            return true;
        }
        
        private (int Index, int Match) SearchInPage(long key)
        {
            var positions = Positions;
            int high = Header->NumberOfEntries - 1, low = 0;
            int match = -1;
            int mid = 0;
            while (low <= high)
            {
                mid = (high + low) / 2;
                var offset = Positions[mid];
                var cur = ZigZag.Decode(Span.Slice(offset));
                match = key.CompareTo(cur);

                if (match == 0)
                {
                    return (mid, 0);
                }

                if (match > 0)
                {
                    low = mid + 1;
                    match = 1;
                }
                else
                {
                    high = mid - 1;
                    match = -1;
                }
            }
            var lastMatch = match > 0 ? 1:-1;
            if (lastMatch == 1)
                mid++;
            return (~mid, lastMatch);
        }
    }
}
