using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Compression;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.PostingLists
{
    public readonly unsafe struct PostingListBranchPage
    {
        private readonly Page _page;

        public PostingListBranchPage(Page page)
        {
            _page = page;
        }

        public long First => ZigZagEncoding.Decode<long>(Span, Positions[0]);
        
        public long Last => ZigZagEncoding.Decode<long>(Span, Positions[^1]);

        public void Init()
        {
            Header->Flags = PageFlags.Single | PageFlags.Other;
            Header->SetFlags = ExtendedPageType.PostingListBranch;
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
        
        public PostingListBranchPageHeader* Header => (PostingListBranchPageHeader*)_page.Pointer;
        
        private Span<ushort> Positions => new Span<ushort>(_page.Pointer + PageHeader.SizeOf, Header->NumberOfEntries);

        private ushort* PositionsPtr => (ushort*)(_page.Pointer + PageHeader.SizeOf);

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
            var header = (PostingListBranchPageHeader*)tmp.TempPagePointer;
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
            private readonly PostingListBranchPage _parent;
            private int _pos;

            public Iterator(PostingListBranchPage parent)
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
                (key, page) = ZigZagEncoding.Decode2<long>(_parent._page.Pointer, out int _, _parent.PositionsPtr[_pos++]);
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

            // PERF: Skipping is much less resource intensive than reading. If we don't need the key, why read it?
            byte* ptr = VariableSizeEncoding.Skip<long>(_page.Pointer + PositionsPtr[actual]);
            long value = ZigZagEncoding.Decode<long>(ptr, out var _);
            return (value, index, match);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (long Key, long Page) GetByIndex(int index)
        {
            return ZigZagEncoding.Decode2<long>(_page.Pointer, out var _, PositionsPtr[index]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPageByIndex(int index)
        {
            // PERF: Skipping is much less resource intensive than reading. If we don't need the key, why read it?
            byte* ptr = VariableSizeEncoding.Skip<long>(_page.Pointer + PositionsPtr[index]);
            return ZigZagEncoding.Decode<long>(ptr, out var _);
        }

        public bool TryGetValue(long key, out long value)
        {
            var (index, match) = SearchInPage(key);
            if (match != 0)
            {
                value = -1;
                return false;
            }
            (_, value) = ZigZagEncoding.Decode2<long>(_page.Pointer, out var _, PositionsPtr[index]);
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
                // PERF: Skipping is much less resource intensive than reading. If we don't need the key, why read it?
                byte* ptr = VariableSizeEncoding.Skip<long>(_page.Pointer + PositionsPtr[index]);

                var existing = ZigZagEncoding.Decode<long>(ptr, out var _);
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
            ushort* @base = PositionsPtr;
            byte* span = _page.Pointer;
            int length = Header->NumberOfEntries;

            if (length == 0)
                return (-1, -1);

            int match;
            int bot = 0;
            int top = length;
            while (top > 1)
            {
                int mid = top / 2;
                match = key.CompareTo(ZigZagEncoding.Decode<long>(span, out _, @base[bot + mid]));

                if (match >= 0)
                    bot += mid;
                top -= mid;
            }

            match = key.CompareTo(ZigZagEncoding.Decode<long>(span, out _, @base[bot]));
            if (match == 0)
                return (bot, 0);

            return (~(bot + (match > 0).ToInt32()), match > 0 ? 1 : -1);
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
