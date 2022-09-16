using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection.Metadata;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Xml;
using Sparrow.Compression;
using Sparrow.Server.Compression;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.Sets
{
    public readonly unsafe struct SetBranchPage
    {
        private readonly Page _page;

        public SetBranchPage(Page page)
        {
            _page = page;
        }

        public long First => ZigZagEncoding.Decode<long>(Span, Positions[0]);
        
        public long Last => ZigZagEncoding.Decode<long>(Span, Positions[^1]);

        public void Init()
        {
            Header->Flags = PageFlags.Single | PageFlags.Other;
            Header->SetFlags = ExtendedPageType.SetBranch;
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
        
        public SetBranchPageHeader* Header => (SetBranchPageHeader*)_page.Pointer;
        
        private Span<ushort> Positions => new Span<ushort>(_page.Pointer + PageHeader.SizeOf, Header->NumberOfEntries);

        private int FreeSpace => Header->Upper - PageHeader.SizeOf - (Header->NumberOfEntries * sizeof(ushort));
        
        private readonly Span<byte> Span => new Span<byte>(_page.Pointer, Constants.Storage.PageSize);
        public int SpaceUsed
        {
            get
            {
                var size = sizeof(ushort) + Positions.Length + PageHeader.SizeOf;
                foreach (ushort position in Positions)
                {
                    ZigZagEncoding.Decode2<long>(Span, out var len, position);
                    size += len;
                }
                return size;
            }
        }


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
                var len = ZigZagEncoding.SizeOf2<long>(entry);

                header->Upper -= (ushort)len;
                Debug.Assert(header->Upper >= endOfPositionsArray);
                entry.Slice(0, len).CopyTo(tmpSpan.Slice(header->Upper));
                positions[i] = header->Upper;
            }
            // zero unused data
            tmpSpan.Slice(endOfPositionsArray, header->Upper - endOfPositionsArray).Clear();
            tmpSpan.CopyTo(Span);
        }

        public List<(long Key, long Page)> GetDebugOutput()
        {
            var results = new List<(long Key, long Page)>();
            var it = Iterate();
            while (it.TryMoveNext(out var key, out var page))
            {
                results.Add((key, page));
            }

            return results;
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

            public bool TryMoveNext(out long key, out long page)
            {
                if (_pos >= _parent.Header->NumberOfEntries)
                {
                    page = -1;
                    key = -1;
                    return false;
                }
                (key, page) = ZigZagEncoding.Decode2<long>(_parent.Span, out int _, _parent.Positions[_pos++]);
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
            var (_, value) = ZigZagEncoding.Decode2<long>(Span, out var _, Positions[actual]);
            return (value, index, match);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (long Key, long Page) GetByIndex(int index)
        {
            return ZigZagEncoding.Decode2<long>(Span, out var _, Positions[index]);
        }

        public bool TryGetValue(long key, out long value)
        {
            var (index, match) = SearchInPage(key);
            if (match != 0)
            {
                value = -1;
                return false;
            }
            (_, value) = ZigZagEncoding.Decode2<long>(Span, out var _, Positions[index]);
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
            var len = ZigZagEncoding.Encode(buffer, key);
            len += ZigZagEncoding.Encode(buffer, page, pos: len);

            var (index, match) = SearchInPage(key);
            if (match ==  0)
            {
                var (_, existing) = ZigZagEncoding.Decode2<long>(Span, out var _, Positions[index]);
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
                var cur = ZigZagEncoding.Decode<long>(Span, offset);
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

        public List<long> GetAllChildPages()
        {
            var results = new List<long>();
            foreach (ushort position in Positions)
            {
                (_, long page) = ZigZagEncoding.Decode2<long>(Span, out var _, position);
                results.Add(page);
            }
            return results;
        }
    }
}
