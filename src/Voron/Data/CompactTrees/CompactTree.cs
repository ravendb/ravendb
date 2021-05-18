using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
using Voron.Debugging;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.CompactTrees
{
    public unsafe class CompactTree
    {
        public const int DictionarySize = 16 * 8;
        private LowLevelTransaction _llt;
        private CompactTreeState _state;
        private CursorState[] _stk = new CursorState[8];
        private int _pos = -1, _len;
        private ByteString _tempBuffer;
        
        private readonly Dictionary<long, PersistentHopeDictionary> _dictionaries = new Dictionary<long, PersistentHopeDictionary>(); 
        
        internal CompactTreeState State => _state;
        internal LowLevelTransaction Llt => _llt;

        private struct Encoder
        {
            public fixed byte Buffer[10];
            public int Length;

            public void ZigZagEncode(long value)
            {
                ulong uv = (ulong)((value << 1) ^ (value >> 63));
                Encode7Bits(uv);
            }
            
            public void Encode7Bits(ulong uv)
            {
                Length = 0;
                while (uv > 0x7Fu)
                {
                    Buffer[Length++] = ((byte)((uint)uv | ~0x7Fu));
                    uv >>= 7;
                }
                Buffer[Length++] = ((byte)uv);
            }

            public static long ZigZagDecode(byte* buffer, out int length)
            {
                ulong result = Decode7Bits(buffer, out length);
                return (long)((result & 1) != 0 ? (result >> 1) - 1 : (result >> 1));
            }
            
            public static ulong Decode7Bits(byte* buffer, out int length)
            {
                ulong result = 0;
                byte byteReadJustNow;
                length = 0;

                const int maxBytesWithoutOverflow = 9;
                for (int shift = 0; shift < maxBytesWithoutOverflow * 7; shift += 7)
                {
                    byteReadJustNow = buffer[length++];
                    result |= (byteReadJustNow & 0x7Ful) << shift;

                    if (byteReadJustNow <= 0x7Fu)
                    {
                        return result;
                    }
                }

                byteReadJustNow = buffer[length];
                if (byteReadJustNow > 0b_1u)
                {
                    throw new ArgumentOutOfRangeException("result", "Bad var int value");
                }

                result |= (ulong)byteReadJustNow << (maxBytesWithoutOverflow * 7);
                return result;
            }
        }
        
        private struct CursorState
        {
            public Page Page;
            public int LastMatch;
            public int LastSearchPosition;
            public CompactPageHeader* Header => (CompactPageHeader*)Page.Pointer;
            public Span<byte> Prefix
            {
                get
                {
                    var p = Page.Pointer + PageHeader.SizeOf;
                    return new Span<byte>(p + 1, p[0]);
                }
            }

            public string DumpPageDebug()
            {
                var sb = new StringBuilder();
                int total = 0;
                for (int i = 0; i < Header->NumberOfEntries; i++)
                {
                    total += GetEntry(Page, i, out var key, out var l);
                    sb.AppendLine($" - {Encoding.UTF8.GetString(key)} - {l}");
                }
                sb.AppendLine($"---- size:{total} ----");
                return sb.ToString();
            }
            public override string ToString()
            {
                if (Page.Pointer == null)
                    return "<null state>";

                return $"{nameof(Page)}: {Page.PageNumber} - {nameof(LastMatch)} : {LastMatch}, " +
                    $"{nameof(LastSearchPosition)} : {LastSearchPosition} - {Header->NumberOfEntries} entries, {Header->Lower}..{Header->Upper}";
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort* GetEntriesOffsets(byte* pagePtr)
        {
            return (ushort*)(pagePtr + PageHeader.SizeOf + DictionarySize);
        }

        private CompactTree()
        {
        }

        public static CompactTree Create(LowLevelTransaction llt, string name)
        {
            using var _ = Slice.From(llt.Allocator, name, out var slice);
            return Create(llt, slice);
        }
        public static CompactTree Create(LowLevelTransaction llt, Slice name)
        {
            CompactTreeState* header;
            var existing = llt.RootObjects.Read(name);
            if (existing == null)
            {
                var dictionaryId = PersistentHopeDictionary.CreateEmpty(llt);
                var newPage = llt.AllocatePage(1);
                var compactPageHeader = (CompactPageHeader*)newPage.Pointer;
                compactPageHeader->PageFlags = CompactPageFlags.Leaf;
                compactPageHeader->Lower = PageHeader.SizeOf + DictionarySize;
                compactPageHeader->Upper = Constants.Storage.PageSize;
                compactPageHeader->FreeSpace = Constants.Storage.PageSize - (PageHeader.SizeOf + DictionarySize);
                compactPageHeader->DictionaryId = dictionaryId;
                using var _ = llt.RootObjects.DirectAdd(name, sizeof(CompactTreeState), out var p);
                header = (CompactTreeState*)p;
                *header = new CompactTreeState
                {
                    RootObjectType = RootObjectType.CompactTree,
                    Depth = 1,
                    Flags = CompactTreeFlags.None,
                    BranchPages = 0,
                    LeafPages = 1,
                    RootPage = newPage.PageNumber,
                    NumberOfEntries = 0,
                };
            }
            else
            {
                header = (CompactTreeState*)existing.Reader.Base;
            }

            if (header->RootObjectType != RootObjectType.CompactTree)
                throw new InvalidOperationException($"Tried to open {name} as a compact tree, but it is actually a " +
                                                    header->RootObjectType);

            return new CompactTree
            {
                _llt = llt,
                _state = *header
            };
        }

        public void Seek(string key)
        {
            using var _ = Slice.From(_llt.Allocator, key, out var slice);
            var span = slice.AsReadOnlySpan();
            Seek(span);
        }
        public void Seek(ReadOnlySpan<byte> key)
        {
            FindPageFor(key);
            ref var state = ref _stk[_pos];
            if (state.LastSearchPosition < 0)
                state.LastSearchPosition = ~state.LastSearchPosition;
        }

        public bool Next(out Span<byte> key, out long value)
        {
            ref var state = ref _stk[_pos];
            while (true)
            {
                Debug.Assert(state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf));
                if (state.LastSearchPosition < state.Header->NumberOfEntries) // same page
                {
                    GetEntry(state.Page, state.LastSearchPosition, out key, out value);
                    state.LastSearchPosition++;
                    return true;
                }
                if (GoToNextPage() == false)
                {
                    key = default;
                    value = default;
                    return false;
                }
            }
        }

        private bool GoToNextPage()
        {
            while (true)
            {
                PopPage(); // go to parent
                if (_pos < 0)
                    return false;

                ref var state = ref _stk[_pos];
                Debug.Assert(state.Header->PageFlags.HasFlag(CompactPageFlags.Branch));
                if (++state.LastSearchPosition >= state.Header->NumberOfEntries)
                    continue; // go up
                do
                {
                    var next = GetValue(ref state, state.LastSearchPosition);
                    PushPage(next);
                    state = ref _stk[_pos];
                } while (state.Header->PageFlags.HasFlag(CompactPageFlags.Branch));
                return true;
            }
        }

        public bool TryGetValue(string key, out long value)
        {
            using var _ = Slice.From(_llt.Allocator, key, out var slice);
            var span = slice.AsReadOnlySpan();
            return TryGetValue(span, out value);

        }

        public bool TryGetValue(ReadOnlySpan<byte> key, out long value)
        {
            FindPageFor(key);
            ref var state = ref _stk[_pos];
            if (state.LastMatch != 0)
            {
                value = default;
                return false;
            }
            value = GetValue(ref state, state.LastSearchPosition);
            return true;
        }

        public bool TryRemove(string key, out long oldValue)
        {
            using var _ = Slice.From(_llt.Allocator, key, out var slice);
            var span = slice.AsReadOnlySpan();
            return TryRemove(span, out oldValue);
        }

        public bool TryRemove(ReadOnlySpan<byte> key, out long oldValue)
        {
            FindPageFor(key);
            return RemoveFromPage(allowRecurse: true, out oldValue);
        }

        private void RemoveFromPage(bool allowRecurse, int pos)
        {
            ref var state = ref _stk[_pos];
            state.LastSearchPosition = pos;
            state.LastMatch = 0;
            RemoveFromPage(allowRecurse, oldValue: out _);
        }
        private bool RemoveFromPage(bool allowRecurse, out long oldValue)
        {
            ref var state = ref _stk[_pos];
            if (state.LastMatch != 0)
            {
                oldValue = default;
                return false;
            }
            state.Page = _llt.ModifyPage(state.Page.PageNumber);

            ushort* entriesOffsets = GetEntriesOffsets(state.Page.Pointer);
            EnsureValidPosition(ref state, state.LastSearchPosition);
            var entry = state.Page.Pointer + entriesOffsets[state.LastSearchPosition];
            var keyLen = (int)Encoder.Decode7Bits(entry, out var lenOfKeyLen);
            entry += keyLen + lenOfKeyLen;
            oldValue = Encoder.ZigZagDecode(entry, out var valLen);
            var totalEntrySize = lenOfKeyLen + keyLen + valLen;
            state.Header->FreeSpace += (ushort)(sizeof(ushort) + totalEntrySize);
             state.Header->Lower -= sizeof(short);// the upper will be fixed on defrag
            Memory.Move((byte*)(entriesOffsets + state.LastSearchPosition),
                (byte*)(entriesOffsets + state.LastSearchPosition + 1),
                (state.Header->NumberOfEntries - state.LastSearchPosition) * sizeof(ushort));
            if (state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
            {
                _state.NumberOfEntries--;
            }
            if (allowRecurse &&
                _pos > 0 && // nothing to do for a single leaf node
                state.Header->FreeSpace > Constants.Storage.PageSize / 3)
            {
                MaybeMergeEntries(ref state);
            }
            return true;
        }

        private void MaybeMergeEntries(ref CursorState state)
        {
            CursorState siblingState;
            ref var parent = ref _stk[_pos - 1];
            // optimization: not merging right most / left most pages
            // that allows to delete in up / down order without doing any
            // merges, for FIFO / LIFO scenarios
            if (parent.LastSearchPosition == 0 ||
                parent.LastSearchPosition == parent.Header->NumberOfEntries - 1)
            {
                if (state.Header->NumberOfEntries == 0) // just remove the whole thing
                {
                    var sibling = GetValue(ref parent, parent.LastSearchPosition == 0 ? 1 : parent.LastSearchPosition - 1);
                    siblingState = new CursorState
                    {
                        Page = _llt.GetPage(sibling)
                    };
                    FreePageFor(ref siblingState, ref state);
                }
                return;
            }
            var siblingPage = GetValue(ref parent, parent.LastSearchPosition + 1);
            siblingState = new CursorState
            {
                Page = _llt.ModifyPage(siblingPage)
            };
            if (siblingState.Header->PageFlags != state.Header->PageFlags)
                return; // cannot merge leaf & branch pages
            ushort* entriesOffsets = GetEntriesOffsets(state.Page.Pointer);
            int entriesCopied = 0;
            for (; entriesCopied < siblingState.Header->NumberOfEntries; entriesCopied++)
            {
                GetEntryBuffer(siblingState.Page, entriesCopied, out var entryBuffer, out var len);
                var requiredSize = len + sizeof(ushort);
                if (requiredSize > state.Header->FreeSpace)
                    break; // done moving entries
               if (requiredSize > state.Header->Upper - state.Header->Lower)
                    DefragPage();
                Debug.Assert(state.Header->Upper >= len);
                state.Header->Upper -= (ushort)len;
                entriesOffsets[state.Header->NumberOfEntries] = state.Header->Upper;
                Memory.Copy(state.Page.Pointer + state.Header->Upper, entryBuffer, len);
                Debug.Assert(state.Header->FreeSpace >= requiredSize);
                state.Header->FreeSpace -= (ushort)requiredSize;
                Debug.Assert(siblingState.Header->FreeSpace + requiredSize <= 
                    Constants.Storage.PageSize - PageHeader.SizeOf - DictionarySize);
                siblingState.Header->FreeSpace += (ushort)requiredSize;
                state.Header->Lower += sizeof(ushort);
            }
            Memory.Move(siblingState.Page.Pointer + PageHeader.SizeOf + DictionarySize,
                siblingState.Page.Pointer + PageHeader.SizeOf + DictionarySize + (entriesCopied * sizeof(ushort)),
                (siblingState.Header->NumberOfEntries - entriesCopied) * sizeof(ushort)
                );
            var oldLower = siblingState.Header->Lower;
            siblingState.Header->Lower -= (ushort)(entriesCopied * sizeof(ushort));
            if (siblingState.Header->NumberOfEntries == 0) // emptied the sibling entriely
            {
                parent.LastSearchPosition++;
                FreePageFor(ref state, ref siblingState);
                return;
            }
            Memory.Set(siblingState.Page.Pointer + siblingState.Header->Lower,
                0, (oldLower - siblingState.Header->Lower));
            // now re-wire the new splitted page key
            var newKey = GetKey(siblingState.Page, 0);
            PopPage();
            // we aren't _really_ removing, so preventing merging of parents
            RemoveFromPage(allowRecurse: false, parent.LastSearchPosition + 1);
            SearchInPage(newKey);// positions changed, re-search
            AddToPage(newKey, siblingPage);
        }

        private void FreePageFor(ref CursorState stateToKeep, ref CursorState stateToDelete)
        {
            ref var parent = ref _stk[_pos - 1];
            DecrementPageNumbers(ref stateToKeep);
            _llt.FreePage(stateToDelete.Page.PageNumber);
            if (parent.Header->NumberOfEntries == 2)
            {   // let's reduce the height of the tree entirely...
                var parentPageNumber = parent.Page.PageNumber;
                Memory.Copy(parent.Page.Pointer, stateToKeep.Page.Pointer, Constants.Storage.PageSize);
                parent.Page.PageNumber = parentPageNumber; // we overwrote it...
                DecrementPageNumbers(ref stateToKeep);
                if (_pos == 1)
                {
                    if (parent.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
                    {
                        _state.LeafPages++;
                        _state.BranchPages--;
                    }
                    _state.Depth--;
                }
                _llt.FreePage(stateToKeep.Page.PageNumber);
                return;
            }
            PopPage();
            RemoveFromPage(allowRecurse: true, parent.LastSearchPosition);
        }

        private void DecrementPageNumbers(ref CursorState state)
        {
            if (state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
            {
                _state.LeafPages--;
            }
            else
            {
                _state.BranchPages--;
            }
        }

        public void Add(string key, long value)
        {
            using var _ = Slice.From(_llt.Allocator, key, out var slice);
            var span = slice.AsReadOnlySpan();
            Add(span, value);
        }

        public void Add(ReadOnlySpan<byte> key, long value)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), "Only positive values are allowed");
            if (key.Length > 1024 || key.Length == 0)
                throw new ArgumentOutOfRangeException(nameof(key), "key must be between 1 and 1024 bytes in size");
            
            var encodedKey = FindPageFor(key);
            AddToPage(encodedKey, value);
        }

        private void AddToPage(ReadOnlySpan<byte> encodedKey, long value)
        {
            ref var state = ref _stk[_pos];
            
            state.Page = _llt.ModifyPage(state.Page.PageNumber);

            var valueEncoder = new Encoder();
            valueEncoder.ZigZagEncode(value);
            ushort* entriesOffsets = GetEntriesOffsets(state.Page.Pointer);
            if (state.LastSearchPosition >= 0) // update
            {
                GetValuePointer(ref state, state.LastSearchPosition, out var b);
                Encoder.ZigZagDecode(b, out var len);
                if (len == valueEncoder.Length)
                {
                    Debug.Assert(valueEncoder.Length <= sizeof(long));
                    Memory.Copy(b, valueEncoder.Buffer, valueEncoder.Length);
                    return;
                }

                // remove the entry, we'll need to add it as new
                Memory.Move((byte*)(entriesOffsets + state.LastSearchPosition - 1),
                    (byte*)(entriesOffsets + state.LastSearchPosition),
                    (state.Header->NumberOfEntries - state.LastSearchPosition) * sizeof(ushort));
                state.Header->Lower -= sizeof(short);
                state.Header->FreeSpace += sizeof(short);
            }
            else
            {
                state.LastSearchPosition = ~state.LastSearchPosition;
            }
            var keySizeEncoder = new Encoder();
            keySizeEncoder.Encode7Bits((ulong)encodedKey.Length);
            var requiredSize = encodedKey.Length + keySizeEncoder.Length + valueEncoder.Length;
            Debug.Assert(state.Header->FreeSpace >= (state.Header->Upper - state.Header->Lower));
            if (state.Header->Upper - state.Header->Lower < requiredSize + sizeof(short))
            {
                if (state.Header->FreeSpace >= requiredSize + sizeof(short))
                    DefragPage(); // has enough free space, but not available try to defrag?
                if (state.Header->Upper - state.Header->Lower < requiredSize + sizeof(short))
                {
                    SplitPage(encodedKey, value); // still can't do that, need to split the page
                    return;
                }
            }
            Memory.Move((byte*)(entriesOffsets + state.LastSearchPosition + 1),
                (byte*)(entriesOffsets + state.LastSearchPosition),
                (state.Header->NumberOfEntries - state.LastSearchPosition) * sizeof(ushort));
            state.Header->Lower += sizeof(short);
            if (state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
                _state.NumberOfEntries++; // we aren't counting branch entries
            Debug.Assert(state.Header->FreeSpace >= requiredSize + sizeof(ushort));
            state.Header->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
            state.Header->Upper -= (ushort)requiredSize;
            byte* writePos = state.Page.Pointer + state.Header->Upper;
            Memory.Copy(writePos, keySizeEncoder.Buffer, keySizeEncoder.Length);
            writePos += keySizeEncoder.Length;
            encodedKey.CopyTo(new Span<byte>(writePos, encodedKey.Length));
            writePos += encodedKey.Length;
            Memory.Copy(writePos, valueEncoder.Buffer, valueEncoder.Length);
            entriesOffsets[state.LastSearchPosition] = state.Header->Upper;
        }

        private void SplitPage(ReadOnlySpan<byte> causeForSplit, long value)
        {
            if (_pos == 0) // need to create a root page
            {
                CreateRootPage();
            }
            var page = _llt.AllocatePage(1);
            var header = (CompactPageHeader*)page.Pointer;
            ref var state = ref _stk[_pos];
            header->PageFlags = state.Header->PageFlags;
            header->Lower = PageHeader.SizeOf + DictionarySize;
            header->Upper = Constants.Storage.PageSize;
            header->FreeSpace = Constants.Storage.PageSize - (PageHeader.SizeOf + DictionarySize);
            header->DictionaryId = state.Header->DictionaryId;
            if (header->PageFlags.HasFlag(CompactPageFlags.Branch))
            {
                _state.BranchPages++;
            }
            else
            {
                _state.LeafPages++;
            }

            var splitKey = SplitPageEntries(causeForSplit, page, header, ref state);
            PopPage(); // add to parent
            SearchInPage(splitKey);
            AddToPage(splitKey, page.PageNumber);
            // now actually add the value to the location
            SearchPageAndPushNext(causeForSplit);
            SearchInPage(causeForSplit);
            AddToPage(causeForSplit, value);
        }

        private ReadOnlySpan<byte> SplitPageEntries(ReadOnlySpan<byte> causeForSplit, Page page,
            CompactPageHeader* header, ref CursorState state)
        {
            // sequential write up, no need to actually split
            int numberOfEntries = state.Header->NumberOfEntries;
            if (numberOfEntries == state.LastSearchPosition && state.LastMatch > 0)
            {
                return causeForSplit;
            }
            // non sequential write, let's just split in middle
            int entriesCopied = 0;
            int sizeCopied = 0;
            ushort* offsets = (ushort*)(page.Pointer + header->Lower);
            for (int i = numberOfEntries / 2; i < numberOfEntries; i++)
            {
                header->Lower += sizeof(ushort);
                GetEntryBuffer(state.Page, i, out var b, out var len);
                header->Upper -= (ushort)len;
                header->FreeSpace -= (ushort)(len + sizeof(ushort));
                sizeCopied += len + sizeof(ushort);
                offsets[entriesCopied++] = header->Upper;
                Memory.Copy(page.Pointer + header->Upper, b, len);
            }
            state.Header->Lower -= (ushort)(sizeof(ushort) * entriesCopied);
            state.Header->FreeSpace += (ushort)(sizeCopied);
            GetEntry(page, 0, out var splitKey, out _);
            return splitKey;
        }

        [Conditional("DEBUG")]
        public void Render()
        {
            DebugStuff.RenderAndShow(this);
        }

        private void CreateRootPage()
        {
            _state.Depth++;
            _state.BranchPages++;
            // we'll copy the current page and reuse it, to avoid changing the root page number
            var page = _llt.AllocatePage(1);
            long cpy = page.PageNumber;
            ref var state = ref _stk[_pos];
            Memory.Copy(page.Pointer, state.Page.Pointer, Constants.Storage.PageSize);
            page.PageNumber = cpy;
            Memory.Set(state.Page.DataPointer, 0, Constants.Storage.PageSize - PageHeader.SizeOf);
            state.Header->PageFlags = CompactPageFlags.Branch;
            state.Header->Lower = DictionarySize + PageHeader.SizeOf + sizeof(ushort);
            state.Header->FreeSpace = Constants.Storage.PageSize - (PageHeader.SizeOf + DictionarySize);

            var encoder = new Encoder();
            encoder.ZigZagEncode(cpy);
            var size = 1 + encoder.Length;
            state.Header->Upper = (ushort)(Constants.Storage.PageSize - size);
            state.Header->FreeSpace -= (ushort)(size + sizeof(ushort));
            GetEntriesOffsets(state.Page.Pointer)[0] = state.Header->Upper;
            byte* entryPos = state.Page.Pointer + state.Header->Upper;
            *entryPos++ = 0; // zero len key
            Memory.Copy(entryPos, encoder.Buffer, encoder.Length);
            InsertToStack(new CursorState
            {
                Page = page,
                LastMatch = state.LastMatch,
                LastSearchPosition = state.LastSearchPosition
            });
            state.LastMatch = -1;
            state.LastSearchPosition = 0;
        }

        private void InsertToStack(CursorState newPageState)
        {
            // insert entry and shift other elements
            if (_len + 1 >= _stk.Length)// should never happen
                Array.Resize(ref _stk, _stk.Length * 2); // but let's handle it
            Array.Copy(_stk, _pos + 1, _stk, _pos + 2, _len - (_pos + 1));
            _len++;
            _stk[_pos + 1] = newPageState;
            _pos++;
        }

        private void DefragPage()
        {
            ref var state = ref _stk[_pos];
    
            using (_llt.Environment.GetTemporaryPage(_llt, out var tmp))
            {
                Memory.Copy(tmp.TempPagePointer, state.Page.Pointer, Constants.Storage.PageSize);
                var tmpHeader = (CompactPageHeader*)tmp.TempPagePointer;
                tmpHeader->Upper = Constants.Storage.PageSize;
                ushort* entriesOffsets = GetEntriesOffsets(tmp.TempPagePointer);
                for (int i = 0; i < state.Header->NumberOfEntries; i++)
                {
                    GetEntryBuffer(state.Page, i, out var b, out var len);
                    Debug.Assert((tmpHeader->Upper - len) > 0);
                    tmpHeader->Upper -= (ushort)len;
                    // Note: FreeSpace doesn't change here
                    Memory.Copy(tmp.TempPagePointer + tmpHeader->Upper, b, len);
                    entriesOffsets[i] = tmpHeader->Upper;
                }
                Memory.Copy(state.Page.Pointer, tmp.TempPagePointer, Constants.Storage.PageSize);
                Memory.Set(state.Page.Pointer + tmpHeader->Lower, 0,
                    tmpHeader->Upper - tmpHeader->Lower);
                Debug.Assert(state.Header->FreeSpace == (state.Header->Upper - state.Header->Lower));
            }
        }

        private Span<Byte> FindPageFor(ReadOnlySpan<byte> key)
        {
            _pos = -1;
            _len = 0;
            PushPage(_state.RootPage);
            ref var state = ref _stk[_pos];
            var currentDictionary = state.Header->DictionaryId;
            var hopeDictionary = _dictionaries[state.Header->DictionaryId];

            if (_tempBuffer.HasValue == false)
            {
                //TODO: Is there a better way? Is a max of twice the size enough?
                _llt.Allocator.Allocate(2048, out _tempBuffer);
            }
            
            var encodedKey = _tempBuffer.ToSpan();
            hopeDictionary.Encode(key, ref encodedKey);
            while (state.Header->PageFlags.HasFlag(CompactPageFlags.Branch))
            {
                SearchPageAndPushNext(encodedKey);
                state = ref _stk[_pos];
                if (currentDictionary != state.Header->DictionaryId)
                {
                    hopeDictionary = _dictionaries[state.Header->DictionaryId];
                    encodedKey =  _tempBuffer.ToSpan();
                    hopeDictionary.Encode(key, ref encodedKey);
                }
            }

            SearchInPage(encodedKey);
            return encodedKey;
        }

        private void SearchPageAndPushNext(ReadOnlySpan<byte> encodedKey)
        {
            SearchInPage(encodedKey);
            ref var state = ref _stk[_pos];
            if (state.LastSearchPosition < 0)
                state.LastSearchPosition = ~state.LastSearchPosition;
            if (state.LastMatch != 0 && state.LastSearchPosition > 0)
                state.LastSearchPosition--; // went too far

            int actualPos = Math.Min(state.Header->NumberOfEntries - 1, state.LastSearchPosition);
            var nextPage = GetValue(ref state, actualPos);
            PushPage(nextPage);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PopPage()
        {
            _stk[_pos--] = default;
            _len--;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PushPage(long nextPage)
        {
            if (_pos + 1 >= _stk.Length) //  should never actually happen
                Array.Resize(ref _stk, _stk.Length * 2); // but let's be safe
            Page page = _llt.GetPage(nextPage);
            _stk[++_pos] = new CursorState { Page = page, };
            _len++;
            CompactPageHeader* pageHeader = (CompactPageHeader*)page.Pointer;
            if (_dictionaries.ContainsKey(pageHeader->DictionaryId) == false)
            {
                _dictionaries[pageHeader->DictionaryId] = CreateDictionary(pageHeader->DictionaryId);
            }
        }

        private PersistentHopeDictionary CreateDictionary(long pageId)
        {
            Page page = _llt.GetPage(pageId);
            Debug.Assert(page.IsOverflow && page.OverflowSize == PersistentHopeDictionary.UsableDictionarySize);
            return new PersistentHopeDictionary(page);
        }

        private ReadOnlySpan<byte> GetKey(Page page, int pos)
        {
            EnsureValidPosition(page, pos);
            ushort entryOffset = GetEntriesOffsets(page.Pointer)[pos];
            var entryPos = page.Pointer + entryOffset;
            var keyLen = Encoder.Decode7Bits(entryPos, out var lenOfKeyLen);
            return new ReadOnlySpan<byte>(page.Pointer + entryOffset + lenOfKeyLen,  (int)keyLen);
        }

        private long GetValue(ref CursorState state, int pos)
        {
            GetValuePointer(ref state, pos, out var p);
            return Encoder.ZigZagDecode(p, out _);
        }

        private void GetValuePointer(ref CursorState state, int pos, out byte* p)
        {
            EnsureValidPosition(ref state, pos);
            ushort entryOffset = GetEntriesOffsets(state.Page.Pointer)[pos];
            p = state.Page.Pointer + entryOffset;
            var keyLen = (int)Encoder.Decode7Bits(p, out var lenKeyLen);
            p += keyLen + lenKeyLen;
        }

        [Conditional("DEBUG")]
        private static void EnsureValidPosition(ref CursorState state, int pos)
        {
            if (pos < 0 || pos >= state.Header->NumberOfEntries)
                throw new ArgumentOutOfRangeException();
        }

        internal static int GetEntry(Page page, int pos, out Span<byte> key, out long value)
        {
            ushort entryOffset = GetEntriesOffsets(page.Pointer)[pos];
            byte* entryPos = page.Pointer + entryOffset;
            var keyLen = (int)Encoder.Decode7Bits(entryPos, out var lenKeyLen);
            key = new Span<byte>(entryPos + lenKeyLen, keyLen);
            entryPos += keyLen + lenKeyLen;
            value = Encoder.ZigZagDecode(entryPos, out var valLen);
            entryPos += valLen;
            return (int)(entryPos - page.Pointer - entryOffset);
        }

        private static void GetEntryBuffer(Page page, int pos, out byte* b, out int len)
        {
            EnsureValidPosition(page, pos);
            ushort entryOffset = GetEntriesOffsets(page.Pointer)[pos];
            byte* entryPos = b = page.Pointer + entryOffset;
            var keyLen = (int)Encoder.Decode7Bits(entryPos, out var lenKeyLen);
            Encoder.ZigZagDecode(entryPos + keyLen + lenKeyLen, out var valLen);
            len = lenKeyLen + keyLen + valLen;
        }

        [Conditional("DEBUG")]
        private static void EnsureValidPosition(Page page, int pos)
        {
            CompactPageHeader* header = (CompactPageHeader*)page.Pointer;
            if (pos < 0 || pos >= header->NumberOfEntries)
                throw new ArgumentOutOfRangeException();
        }

        private void SearchInPage(ReadOnlySpan<byte> encodedKey)
        {
            ref var state = ref _stk[_pos];

            int high = state.Header->NumberOfEntries - 1, low = 0;
            int match = -1;
            int mid = 0;
            while (low <= high)
            {
                mid = (high + low) / 2;
                var cur = GetKey(state.Page, mid);
                match = encodedKey.SequenceCompareTo(cur);

                if (match == 0)
                {
                    state.LastMatch = 0;
                    state.LastSearchPosition = mid;
                    return;
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
            state.LastMatch = match > 0 ? 1 : -1;
            if (match > 0)
                mid++;
            state.LastSearchPosition = ~mid;
        }
    }
}
