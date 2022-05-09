using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
using Voron.Data.BTrees;
using Voron.Debugging;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.CompactTrees
{
    public unsafe partial class CompactTree
    {
        private LowLevelTransaction _llt;
        private CompactTreeState _state;

        private struct IteratorCursorState
        {
            internal CursorState[] _stk;
            internal int _pos;
            internal int _len;
        }

        // TODO: Improve interactions with caller code. It is good enough for now but we can encapsulate behavior better to improve readability. 
        private IteratorCursorState _internalCursor = new() { _stk = new CursorState[8], _pos = -1, _len = 0 };

        // TODO: We will never rewrite a dictionary, only create new ones. Therefore, we can effectively cache them until removing them. 
        private readonly Dictionary<long, PersistentDictionary> _dictionaries = new(); 
        
        internal CompactTreeState State => _state;
        internal LowLevelTransaction Llt => _llt;

        private struct TreePageList : IReadOnlySpanEnumerator
        {
            private int _currentIdx = 0;
            private readonly CompactTree _tree;
            private readonly CursorState _state;
            private readonly PersistentDictionary _dictionary;

            public TreePageList(CompactTree tree, CursorState state, PersistentDictionary dictionary)
            {
                _tree = tree;
                _state = state;
                _dictionary = dictionary;
            }

            public int Length => _state.Header->NumberOfEntries;

            public bool IsNull(int i)
            {
                if (i < 0 || i >= Length)
                    throw new IndexOutOfRangeException();
                return false;
            }

            private unsafe ReadOnlySpan<byte> this[int i]
            {
                get
                {
                    var encodedKey = GetEncodedKey(_state.Page, _state.EntriesOffsets[i]);
                    _tree.Llt.Allocator.Allocate(_dictionary.GetMaxDecodingBytes(encodedKey), out var tempBuffer);

                    var key = tempBuffer.ToSpan();
                    _dictionary.Decode(encodedKey, ref key);
                    return key;
                }
            }

            public void Reset()
            {
                _currentIdx = 0;
            }

            public bool MoveNext(out ReadOnlySpan<byte> result)
            {
                if (_currentIdx >= _state.Header->NumberOfEntries)
                {
                    result = default;
                    return false;
                }

                result = this[_currentIdx++];
                return true;
            }
        }

        private readonly ref struct EncodedKey
        {
            public readonly ReadOnlySpan<byte> Key;
            public readonly ReadOnlySpan<byte> Encoded;
            public readonly long Dictionary;

            private EncodedKey(ReadOnlySpan<byte> key, ReadOnlySpan<byte> encodedKey, long dictionary)
            {
                Key = key;
                Encoded = encodedKey;
                Dictionary = dictionary;
            }

            public override string ToString()
            {
                return Encoding.UTF8.GetString(Key);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static EncodedKey Get(EncodedKey encodedKey, CompactTree tree, long dictionaryId)
            {
                if (dictionaryId == encodedKey.Dictionary)
                    return encodedKey;

                var key = encodedKey.Key;
                return Get(key, tree, dictionaryId);
            }

            public static EncodedKey Get(ReadOnlySpan<byte> key, CompactTree tree, long dictionaryId)
            {
                var dictionary = tree.GetEncodingDictionary(dictionaryId);

                tree.Llt.Allocator.Allocate(dictionary.GetMaxEncodingBytes(key), out var encodedKey);

                var encodedKeySpan = encodedKey.ToSpan();
                dictionary.Encode(key, ref encodedKeySpan);

                return new EncodedKey(key, encodedKeySpan, dictionaryId);
            }

            public static EncodedKey From(ReadOnlySpan<byte> encodedKey, CompactTree tree, long dictionaryId)
            {
                var dictionary = tree.GetEncodingDictionary(dictionaryId);

                tree.Llt.Allocator.Allocate(dictionary.GetMaxDecodingBytes(encodedKey), out var unencodedKey);
                var unencodedKeySpan = unencodedKey.ToSpan();
                dictionary.Decode(encodedKey, ref unencodedKeySpan);

                return new EncodedKey(unencodedKeySpan, encodedKey, dictionaryId);
            }
        }

        // TODO: Rewrite this in the BinaryWriter way to make it even more direct writing to the end location directly and no need to copy.
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
            
            public Span<ushort> EntriesOffsets => new Span<ushort>(Page.Pointer+ PageHeader.SizeOf, Header->NumberOfEntries);

            public string DumpPageDebug(CompactTree tree)
            {
                var dictionary = tree._dictionaries[Header->DictionaryId];

                Span<byte> tempBuffer = stackalloc byte[2048];

                var sb = new StringBuilder();
                int total = 0;
                for (int i = 0; i < Header->NumberOfEntries; i++)
                {
                    total += GetEncodedEntry(Page, EntriesOffsets[i], out var key, out var l);

                    var decodedKey = tempBuffer;
                    dictionary.Decode(key, ref decodedKey);

                    sb.AppendLine($" - {Encoding.UTF8.GetString(decodedKey)} - {l}");
                }
                sb.AppendLine($"---- size:{total} ----");
                return sb.ToString();
            }

            public string DumpRawPageDebug()
            {
                var sb = new StringBuilder();
                int total = 0;
                for (int i = 0; i < Header->NumberOfEntries; i++)
                {
                    total += GetEncodedEntry(Page, EntriesOffsets[i], out var key, out var l);
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
        
        private CompactTree(Slice name, Tree parent)
        {
            Name = name.Clone(parent.Llt.Allocator, ByteStringType.Immutable);
            _parent = parent;
        }

        public Slice Name 
        { 
            get; 
            private set;
        }

        private Tree _parent;

        public long NumberOfEntries => _state.NumberOfEntries;

        public static CompactTree Create(LowLevelTransaction llt, string name)
        {
            return llt.RootObjects.CompactTreeFor(name);
        }

        public static CompactTree Create(LowLevelTransaction llt, Slice name)
        {
            return llt.RootObjects.CompactTreeFor(name);
        }

        public static CompactTree Create(Tree parent, string name)
        {
            return parent.CompactTreeFor(name);
        }

        public static CompactTree Create(Tree parent, Slice name)
        {
            return parent.CompactTreeFor(name);
        }

        public static CompactTree InternalCreate(Tree parent, Slice name)
        {
            var llt = parent.Llt;

            CompactTreeState* header;

            var existing = parent.Read(name);
            if (existing == null)
            {
                if (llt.Flags != TransactionFlags.ReadWrite)
                    return null;

                // This will be created a single time and stored in the root page.                 
                var dictionaryId = PersistentDictionary.CreateDefault(llt);

                var newPage = llt.AllocatePage(1);
                var compactPageHeader = (CompactPageHeader*)newPage.Pointer;
                compactPageHeader->PageFlags = CompactPageFlags.Leaf;
                compactPageHeader->Lower = PageHeader.SizeOf;
                compactPageHeader->Upper = Constants.Storage.PageSize;
                compactPageHeader->FreeSpace = Constants.Storage.PageSize - (PageHeader.SizeOf);
                compactPageHeader->DictionaryId = dictionaryId;

                using var _ = parent.DirectAdd(name, sizeof(CompactTreeState), out var p);
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
                    TreeDictionaryId = dictionaryId,
#if DEBUG
                    NextTrainAt = 5000,  // We want this to run far more often in debug to know we are exercising.
#else
                    NextTrainAt = 100000, // We wont try to train the encoder until we have more than 100K entries
#endif
                };
            }
            else
            {
                header = (CompactTreeState*)existing.Reader.Base;
            }

            if (header->RootObjectType != RootObjectType.CompactTree)
                throw new InvalidOperationException($"Tried to open {name} as a compact tree, but it is actually a " +
                                                    header->RootObjectType);

            return new CompactTree(name, parent)
            {
                _llt = llt,
                _state = *header
            };
        }

        public void PrepareForCommit()
        {
            if (_state.NumberOfEntries >= _state.NextTrainAt)
            {
                // Because the size of the tree is really big, we are better of sampling less amount of data because unless we
                // grow the table size there is probably not much improvement we can do adding more samples. Only way to improve
                // under those conditions is in finding a better sample for training. 
                long sampleSize = (_state.NumberOfEntries > 32 * 1_000_000) ? 
                    _state.NumberOfEntries / 100 :  
                    _state.NumberOfEntries / 10;

                TryImproveDictionaryByRandomlyScanning(sampleSize);
            }

            using var _ = _parent.DirectAdd(Name, sizeof(CompactTreeState), out var ptr);
            _state.CopyTo((CompactTreeState*)ptr);
        }

        public static void Delete(CompactTree tree)
        {
            Delete(tree, tree.Llt.RootObjects);
        }

        public static void Delete(CompactTree tree, Tree parent)
        {
            throw new NotImplementedException();
        }

        private bool GoToNextPage(ref IteratorCursorState cstate)
        {
            while (true)
            {
                PopPage(ref cstate); // go to parent
                if (cstate._pos < 0)
                    return false;

                ref var state = ref cstate._stk[cstate._pos];
                Debug.Assert(state.Header->PageFlags.HasFlag(CompactPageFlags.Branch));
                if (++state.LastSearchPosition >= state.Header->NumberOfEntries)
                    continue; // go up
                do
                {
                    var next = GetValue(ref state, state.LastSearchPosition);
                    PushPage(next, ref cstate);
                    state = ref cstate._stk[cstate._pos];
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
            var encodedKey = FindPageFor(key, ref _internalCursor);

            ref var state = ref _internalCursor._stk[_internalCursor._pos];

            // Ensure that the key has already been 'updated' this is internal and shouldn't check explicitly that.
            // It is the responsibility of the caller to ensure that is the case. 
            Debug.Assert(encodedKey.Dictionary == state.Header->DictionaryId);

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

        public bool TryRemove(Slice key, out long oldValue)
        {
            return TryRemove(key.AsReadOnlySpan(), out oldValue);
        }

        public bool TryRemove(ReadOnlySpan<byte> key, out long oldValue)
        {
            FindPageFor(key, ref _internalCursor);
            return RemoveFromPage(allowRecurse: true, out oldValue);
        }

        private void RemoveFromPage(bool allowRecurse, int pos)
        {
            ref var state = ref _internalCursor._stk[_internalCursor._pos];
            state.LastSearchPosition = pos;
            state.LastMatch = 0;
            RemoveFromPage(allowRecurse, oldValue: out _);
        }

        private bool RemoveFromPage(bool allowRecurse, out long oldValue)
        {
            ref var state = ref _internalCursor._stk[_internalCursor._pos];
            if (state.LastMatch != 0)
            {
                oldValue = default;
                return false;
            }
            state.Page = _llt.ModifyPage(state.Page.PageNumber);

            var entriesOffsets = state.EntriesOffsets;
            var entry = state.Page.Pointer + entriesOffsets[state.LastSearchPosition];

            var keyLen = (int)Encoder.Decode7Bits(entry, out var lenOfKeyLen);
            entry += keyLen + lenOfKeyLen;
            oldValue = Encoder.ZigZagDecode(entry, out var valLen);

            var totalEntrySize = lenOfKeyLen + keyLen + valLen;
            state.Header->FreeSpace += (ushort)(sizeof(ushort) + totalEntrySize);
            state.Header->Lower -= sizeof(short); // the upper will be fixed on defrag
            entriesOffsets[(state.LastSearchPosition + 1)..].CopyTo(entriesOffsets[state.LastSearchPosition..]);
            
            if (state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
            {
                _state.NumberOfEntries--;
            }

            if (allowRecurse &&
                _internalCursor._pos > 0 && // nothing to do for a single leaf node
                state.Header->FreeSpace > Constants.Storage.PageSize / 3)
            {
                MaybeMergeEntries(ref state);
            }

            return true;
        }

        private void Verify()
        {
            ref var current = ref _internalCursor._stk[_internalCursor._pos];

            var dictionary = _dictionaries[current.Header->DictionaryId];

            int len = GetEncodedEntry(current.Page, current.EntriesOffsets[0], out var lastEncodedKey, out var l);

            Span<byte> lastDecodedKey = new byte[dictionary.GetMaxDecodingBytes(lastEncodedKey)];
            dictionary.Decode(lastEncodedKey, ref lastDecodedKey);

            for (int i = 1; i < current.Header->NumberOfEntries; i++)
            {
                GetEncodedEntry(current.Page, current.EntriesOffsets[i], out var encodedKey, out l);
                Debug.Assert(encodedKey.Length > 0);
                Debug.Assert(lastEncodedKey.SequenceCompareTo(encodedKey) < 0);

                Span<byte> decodedKey = new byte[dictionary.GetMaxDecodingBytes(encodedKey)];
                dictionary.Decode(encodedKey, ref decodedKey);

                Span<byte> decodedKey1 = new byte[dictionary.GetMaxDecodingBytes(encodedKey)];
                dictionary.Decode(encodedKey, ref decodedKey1);

                if (decodedKey1.SequenceCompareTo(decodedKey) != 0)
                    Debug.Fail("");

                // Console.WriteLine($"{Encoding.UTF8.GetString(lastDecodedKey)} - {Encoding.UTF8.GetString(decodedKey)}");

                if (lastDecodedKey.SequenceCompareTo(decodedKey) > 0)
                {
                    Console.WriteLine($"{Encoding.UTF8.GetString(lastDecodedKey)} - {Encoding.UTF8.GetString(decodedKey)}");

                    decodedKey = new byte[dictionary.GetMaxDecodingBytes(encodedKey)];
                    dictionary.Decode(encodedKey, ref decodedKey);

                    dictionary.Decode(lastEncodedKey, ref lastDecodedKey);
                    Debug.Fail("");
                }
                
                lastEncodedKey = encodedKey;
                lastDecodedKey = decodedKey;
            }
        }

        private void MaybeMergeEntries(ref CursorState destinationState)
        {
            CursorState sourceState;
            ref var parent = ref _internalCursor._stk[_internalCursor._pos - 1];

            // optimization: not merging right most / left most pages
            // that allows to delete in up / down order without doing any
            // merges, for FIFO / LIFO scenarios
            if (parent.LastSearchPosition == 0 ||
                parent.LastSearchPosition == parent.Header->NumberOfEntries - 1)
            {
                if (destinationState.Header->NumberOfEntries == 0) // just remove the whole thing
                {
                    var sibling = GetValue(ref parent, parent.LastSearchPosition == 0 ? 1 : parent.LastSearchPosition - 1);
                    sourceState = new CursorState
                    {
                        Page = _llt.GetPage(sibling)
                    };
                    FreePageFor(ref sourceState, ref destinationState);
                }
                return;
            }

            var siblingPage = GetValue(ref parent, parent.LastSearchPosition + 1);
            sourceState = new CursorState
            {
                Page = _llt.ModifyPage(siblingPage)
            };

            if (sourceState.Header->PageFlags != destinationState.Header->PageFlags)
                return; // cannot merge leaf & branch pages

            using var __ = _llt.Allocator.Allocate(4096, out var buffer);
            var decodeBuffer = new Span<byte>(buffer.Ptr, 2048);
            var encodeBuffer = new Span<byte>(buffer.Ptr + 2048, 2048);

            var destinationPage = destinationState.Page;
            var destinationHeader = destinationState.Header;
            var sourcePage = sourceState.Page;
            var sourceHeader = sourceState.Header;

            // the new entries size is composed of entries from _both_ pages            
            var entries = new Span<ushort>(destinationPage.Pointer + PageHeader.SizeOf, destinationHeader->NumberOfEntries + sourceHeader->NumberOfEntries)
                .Slice(destinationHeader->NumberOfEntries);

            var srcDictionary = GetEncodingDictionary(sourceHeader->DictionaryId);
            var destDictionary = GetEncodingDictionary(destinationHeader->DictionaryId);
            bool reencode = sourceHeader->DictionaryId != destinationHeader->DictionaryId;

            int sourceEncodedKeysLenght = 0;
            int sourceKeysCopied = 0;
            for (; sourceKeysCopied < sourceHeader->NumberOfEntries; sourceKeysCopied++)
            {
                // We get the encoded key and value from the sibling page
                GetEncodedEntry(sourcePage, sourceState.EntriesOffsets[sourceKeysCopied], out var encodedKey, out var val);

                sourceEncodedKeysLenght += encodedKey.Length;
                
                // If they have a different dictionary, we need to re-encode the entry with the new dictionary.
                if (reencode && encodedKey.Length != 0)
                {
                    var decodedKey = decodeBuffer;
                    srcDictionary.Decode(encodedKey, ref decodedKey);

                    encodedKey = encodeBuffer;
                    destDictionary.Encode(decodedKey, ref encodedKey);
                }

                // We encode the length of the key and the value with variable length in order to store them later. 
                var valueEncoder = new Encoder();
                valueEncoder.ZigZagEncode(val);

                var keySizeEncoder = new Encoder();
                keySizeEncoder.Encode7Bits((ulong)encodedKey.Length);

                // If we dont have enough free space in the receiving page, we move on. 
                var requiredSize = encodedKey.Length + keySizeEncoder.Length + valueEncoder.Length;
                if (requiredSize + sizeof(ushort) > destinationHeader->FreeSpace)
                    break; // done moving entries

                // However, there can be enough space in the receiving page but would need a defrag to be able to do so. 
                if (requiredSize + sizeof(ushort) > destinationState.Header->Upper - destinationState.Header->Lower)
                {
                    // DebugStuff.RenderAndShow(this);
                    DefragPage();
                }

                // We will update the entries offsets in the receiving page.
                destinationHeader->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
                destinationHeader->Upper -= (ushort)requiredSize;
                destinationHeader->Lower += sizeof(ushort);
                entries[sourceKeysCopied] = destinationHeader->Upper;
                
                // We copy the actual entry <key_size, key, value> to the receiving page.
                var entryPos = destinationPage.Pointer + destinationHeader->Upper;                
                Memory.Copy(entryPos, keySizeEncoder.Buffer, keySizeEncoder.Length);
                entryPos += keySizeEncoder.Length;
                encodedKey.CopyTo(new Span<byte>(entryPos, (int)(destinationPage.Pointer + Constants.Storage.PageSize - entryPos)));
                entryPos += encodedKey.Length;
                Memory.Copy(entryPos, valueEncoder.Buffer, valueEncoder.Length);                                                                

                Debug.Assert(destinationHeader->Upper >= destinationHeader->Lower);
            }

            Memory.Move(sourcePage.Pointer + PageHeader.SizeOf,
                        sourcePage.Pointer + PageHeader.SizeOf + (sourceKeysCopied * sizeof(ushort)),
                        (sourceHeader->NumberOfEntries - sourceKeysCopied) * sizeof(ushort));
            
            // We update the entries offsets on the source page, now that we have moved the entries.
            var oldLower = sourceHeader->Lower;
            sourceHeader->Lower -= (ushort)(sourceKeysCopied * sizeof(ushort));
            sourceHeader->FreeSpace += (ushort)(sourceEncodedKeysLenght + (sourceKeysCopied * sizeof(ushort)));
            if (sourceHeader->NumberOfEntries == 0) // emptied the sibling entries
            {
                parent.LastSearchPosition++;
                FreePageFor(ref destinationState, ref sourceState);
                return;
            }
            
            Memory.Set(sourcePage.Pointer + sourceHeader->Lower, 0, (oldLower - sourceHeader->Lower));

            // now re-wire the new splitted page key
            var newEncodedKey = GetEncodedKey(sourcePage, sourceState.EntriesOffsets[0]);
            var newKey = EncodedKey.From(newEncodedKey, this, sourceHeader->DictionaryId);

            PopPage(ref _internalCursor);
            
            // we aren't _really_ removing, so preventing merging of parents
            RemoveFromPage(allowRecurse: false, parent.LastSearchPosition + 1);

            // Ensure that we got the right key to search. 
            newKey = EncodedKey.Get(newKey, this, _internalCursor._stk[_internalCursor._pos].Header->DictionaryId);
            SearchInCurrentPage(newKey, ref _internalCursor._stk[_internalCursor._pos]);// positions changed, re-search
            AddToPage(newKey, siblingPage);
        }

        private void FreePageFor(ref CursorState stateToKeep, ref CursorState stateToDelete)
        {
            ref var parent = ref _internalCursor._stk[_internalCursor._pos - 1];
            DecrementPageNumbers(ref stateToKeep);
            _llt.FreePage(stateToDelete.Page.PageNumber);
            if (parent.Header->NumberOfEntries == 2)
            {   // let's reduce the height of the tree entirely...
                var parentPageNumber = parent.Page.PageNumber;
                Memory.Copy(parent.Page.Pointer, stateToKeep.Page.Pointer, Constants.Storage.PageSize);
                parent.Page.PageNumber = parentPageNumber; // we overwrote it...
                DecrementPageNumbers(ref stateToKeep);
                if (_internalCursor._pos == 1)
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
            PopPage(ref _internalCursor);
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
                throw new ArgumentOutOfRangeException(nameof(value), value, "Only positive values are allowed");
            if (key.Length > 1024)
                throw new ArgumentOutOfRangeException(nameof(key), Encoding.UTF8.GetString(key),"key must be less than 1024 bytes in size");
            if(key.Length <= 0)
                throw new ArgumentOutOfRangeException(nameof(key), Encoding.UTF8.GetString(key), "key must be at least 1 byte");

            var encodedKey = FindPageFor(key, ref _internalCursor);
            AddToPage(encodedKey, value);
        }

        private EncodedKey AddToPage(EncodedKey key, long value)
        {
            ref var state = ref _internalCursor._stk[_internalCursor._pos];

            // Ensure that the key has already been 'updated' this is internal and shouldn't check explicitly that.
            // It is the responsibility of the caller to ensure that is the case. 
            Debug.Assert(key.Dictionary == state.Header->DictionaryId);

            state.Page = _llt.ModifyPage(state.Page.PageNumber);

            var valueEncoder = new Encoder();
            valueEncoder.ZigZagEncode(value);
            var entriesOffsets = state.EntriesOffsets;
            if (state.LastSearchPosition >= 0) // update
            {
                GetValuePointer(ref state, state.LastSearchPosition, out var b);
                Encoder.ZigZagDecode(b, out var len);
                if (len == valueEncoder.Length)
                {
                    Debug.Assert(valueEncoder.Length <= sizeof(long));
                    Memory.Copy(b, valueEncoder.Buffer, valueEncoder.Length);
                    return key;
                }

                // remove the entry, we'll need to add it as new
                entriesOffsets[(state.LastSearchPosition+1)..].CopyTo(entriesOffsets[state.LastSearchPosition..]);

                state.Header->Lower -= sizeof(short);
                state.Header->FreeSpace += sizeof(short);
                entriesOffsets = state.EntriesOffsets;
                if (state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
                    _state.NumberOfEntries--; // we aren't counting branch entries
            }
            else
            {
                state.LastSearchPosition = ~state.LastSearchPosition;
            }

            var keySizeEncoder = new Encoder();
            keySizeEncoder.Encode7Bits((ulong)key.Encoded.Length);
            var requiredSize = key.Encoded.Length + keySizeEncoder.Length + valueEncoder.Length;
            Debug.Assert(state.Header->FreeSpace >= (state.Header->Upper - state.Header->Lower));
            if (state.Header->Upper - state.Header->Lower < requiredSize + sizeof(short))
            {
                // In splits we will always recompress IF the page has been compressed with an older dictionary.
                bool splitAnyways = true;
                if (state.Header->DictionaryId != _state.TreeDictionaryId)
                {
                    // We will recompress with the new dictionary, which has the side effect of also reclaiming the
                    // free space if there is any available. It may very well happen that this new dictionary
                    // is not as good as the old for this particular page (rare but it can happen). In those cases,
                    // we will not upgrade and just split the pages using the old dictionary. Eventually the new
                    // dictionary will catch up. 
                    if (TryRecompressPage(state))
                    {
                        // It may happen that between the more effective dictionary and the reclaimed space we have enough
                        // to avoid the split. 
                        if (state.Header->Upper - state.Header->Lower >= requiredSize + sizeof(short))
                            splitAnyways = false;

                        key = EncodedKey.Get(key, this, state.Header->DictionaryId);

                        // We need to recompute this because it will change.
                        keySizeEncoder = new Encoder();
                        keySizeEncoder.Encode7Bits((ulong)key.Encoded.Length);
                        requiredSize = key.Encoded.Length + keySizeEncoder.Length + valueEncoder.Length;
                        Debug.Assert(state.Header->FreeSpace >= (state.Header->Upper - state.Header->Lower));
                    }
                }
                else
                {
                    // If we are not recompressing, but we still have enough free space available we will go for reclaiming space
                    // by rearranging unused space.
                    if (state.Header->FreeSpace >= requiredSize + sizeof(short))
                    {
                        DefragPage();
                        splitAnyways = false;
                    }
                }

                if (splitAnyways)
                {
                    // DebugStuff.RenderAndShow(this);
                    return SplitPage(key, value); // still can't do that, need to split the page
                    // DebugStuff.RenderAndShow(this);

                }
            }

            // Ensure that the key has already been 'updated' this is internal and shouldn't check explicitly that.
            // It is the responsibility of the method to ensure that is the case. 
            Debug.Assert(key.Dictionary == state.Header->DictionaryId);

            state.Header->Lower += sizeof(short);
            var newEntriesOffsets = state.EntriesOffsets;
            entriesOffsets[state.LastSearchPosition..].CopyTo(newEntriesOffsets[(state.LastSearchPosition + 1)..]);
            if (state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
                _state.NumberOfEntries++; // we aren't counting branch entries
            Debug.Assert(state.Header->FreeSpace >= requiredSize + sizeof(ushort));
            state.Header->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
            state.Header->Upper -= (ushort)requiredSize;
            byte* writePos = state.Page.Pointer + state.Header->Upper;
            Memory.Copy(writePos, keySizeEncoder.Buffer, keySizeEncoder.Length);
            writePos += keySizeEncoder.Length;
            key.Encoded.CopyTo(new Span<byte>(writePos, key.Encoded.Length));
            writePos += key.Encoded.Length;
            Memory.Copy(writePos, valueEncoder.Buffer, valueEncoder.Length);
            newEntriesOffsets[state.LastSearchPosition] = state.Header->Upper;

            return key;
        }

        private EncodedKey SplitPage(EncodedKey causeForSplit, long value)
        {
            if (_internalCursor._pos == 0) // need to create a root page
            {
                // We are going to be creating a root page with our first trained dictionary. 
                CreateRootPage();
            }

            // We create the new dictionary 
            ref var state = ref _internalCursor._stk[_internalCursor._pos];

            // Ensure that the key has already been 'updated' this is internal and shouldn't check explicitly that.
            // It is the responsibility of the caller to ensure that is the case. 
            Debug.Assert(causeForSplit.Dictionary == state.Header->DictionaryId);

            var page = _llt.AllocatePage(1);
            var header = (CompactPageHeader*)page.Pointer;
            header->PageFlags = state.Header->PageFlags;
            header->Lower = PageHeader.SizeOf ;
            header->Upper = Constants.Storage.PageSize;
            header->FreeSpace = Constants.Storage.PageSize - PageHeader.SizeOf;
            
            if (header->PageFlags.HasFlag(CompactPageFlags.Branch))
            {
                _state.BranchPages++;
            }
            else
            {
                _state.LeafPages++;
            }

            // We do this after in case we have been able to gain with compression.
            header->DictionaryId = state.Header->DictionaryId;

            // We need to ensure that we have the correct key before we change the page. 
            var splitKey = SplitPageEncodedEntries(causeForSplit, page, header, ref state);

            PopPage(ref _internalCursor); // add to parent
            splitKey = EncodedKey.Get(splitKey, this, _internalCursor._stk[_internalCursor._pos].Header->DictionaryId);

            SearchInCurrentPage(splitKey, ref _internalCursor._stk[_internalCursor._pos]);
            AddToPage(splitKey, page.PageNumber);

            // now actually add the value to the location
            causeForSplit = EncodedKey.Get(causeForSplit, this, _internalCursor._stk[_internalCursor._pos].Header->DictionaryId);
            causeForSplit = SearchPageAndPushNext(causeForSplit, ref _internalCursor);

            SearchInCurrentPage(causeForSplit, ref _internalCursor._stk[_internalCursor._pos]);
            causeForSplit = AddToPage(causeForSplit, value);
            return causeForSplit;
        }

        private EncodedKey SplitPageEncodedEntries(EncodedKey causeForSplit, Page page, CompactPageHeader* header, ref CursorState state)
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
                GetEntryBuffer(state.Page, state.EntriesOffsets[i], out var b, out var len);
                header->Upper -= (ushort)len;
                header->FreeSpace -= (ushort)(len + sizeof(ushort));
                sizeCopied += len + sizeof(ushort);
                offsets[entriesCopied++] = header->Upper;
                Memory.Copy(page.Pointer + header->Upper, b, len);
            }
            state.Header->Lower -= (ushort)(sizeof(ushort) * entriesCopied);
            state.Header->FreeSpace += (ushort)(sizeCopied);
            var pageEntries = new Span<ushort>(page.Pointer + PageHeader.SizeOf, header->NumberOfEntries);
            GetEncodedEntry(page, pageEntries[0], out var splitKey, out _);

            return EncodedKey.From(splitKey, this, ((CompactPageHeader*)page.Pointer)->DictionaryId);
        }

        [Conditional("DEBUG")]
        public void Render()
        {
            DebugStuff.RenderAndShow(this);
        }

        private bool TryRecompressPage(in CursorState state)
        {
            var oldDictionary = _dictionaries[state.Header->DictionaryId];
            var newDictionary = GetEncodingDictionary(_state.TreeDictionaryId);                

            using var _ = _llt.Environment.GetTemporaryPage(_llt, out var tmp);
            Memory.Copy(tmp.TempPagePointer, state.Page.Pointer, Constants.Storage.PageSize);

            // TODO: Remove
            // new CursorState() {Page = new Page(tmp.TempPagePointer)}.DumpPageDebug(this);

            Memory.Set(state.Page.DataPointer, 0, Constants.Storage.PageSize - PageHeader.SizeOf);
            state.Header->Upper = Constants.Storage.PageSize;
            state.Header->Lower = PageHeader.SizeOf;
            state.Header->FreeSpace = (ushort)(state.Header->Upper - state.Header->Lower);

            using var __ = _llt.Allocator.Allocate(4096, out var buffer);
            var decodeBuffer = new Span<byte>(buffer.Ptr, 2048);
            var encodeBuffer = new Span<byte>(buffer.Ptr + 2048, 2048);

            var tmpHeader = (CompactPageHeader*)tmp.TempPagePointer;

            var oldEntries = new Span<ushort>(tmp.TempPagePointer + PageHeader.SizeOf, tmpHeader->NumberOfEntries);
            var newEntries = new Span<ushort>(state.Page.Pointer + PageHeader.SizeOf, tmpHeader->NumberOfEntries);
            var tmpPage = new Page(tmp.TempPagePointer);

            for (int i = 0; i < tmpHeader->NumberOfEntries; i++)
            {
                GetEncodedEntry(tmpPage, oldEntries[i], out var encodedKey, out var val);

                if (encodedKey.Length != 0)
                {
                    var decodedKey = decodeBuffer;
                    oldDictionary.Decode(encodedKey, ref decodedKey);

                    encodedKey = encodeBuffer;
                    newDictionary.Encode(decodedKey, ref encodedKey);
                }

                var valueEncoder = new Encoder();
                valueEncoder.ZigZagEncode(val);

                var keySizeEncoder = new Encoder();
                keySizeEncoder.Encode7Bits((ulong)encodedKey.Length);

                // It may very well happen that there is no enough encoding space to upgrade the page
                // because of an slightly inefficiency at this particular page. In those cases, we wont
                // upgrade the page and just fail. 
                var requiredSize = encodedKey.Length + keySizeEncoder.Length + valueEncoder.Length;
                if (requiredSize + sizeof(ushort) > state.Header->FreeSpace)
                    goto Failure;

                state.Header->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
                state.Header->Lower += sizeof(ushort);
                state.Header->Upper -= (ushort)requiredSize;
                newEntries[i] = state.Header->Upper;
                var entryPos = state.Page.Pointer + state.Header->Upper;
                Memory.Copy(entryPos, keySizeEncoder.Buffer, keySizeEncoder.Length);
                entryPos += keySizeEncoder.Length;
                encodedKey.CopyTo(new Span<byte>(entryPos, (int)(state.Page.Pointer + Constants.Storage.PageSize - entryPos)));
                entryPos += encodedKey.Length;
                Memory.Copy(entryPos, valueEncoder.Buffer, valueEncoder.Length);
            }

            Debug.Assert(state.Header->FreeSpace == (state.Header->Upper - state.Header->Lower));

            state.Header->DictionaryId = newDictionary.PageNumber;
            _dictionaries[newDictionary.PageNumber] = newDictionary;

            return true;

            Failure:
            // We will free the page, we will no longer use it.
            // TODO: Probably it is best to just not allocate and copy the page afterwards if we use it. 
            Llt.FreePage(newDictionary.PageNumber);
            Memory.Copy(state.Page.Pointer, tmp.TempPagePointer, Constants.Storage.PageSize);
            return false;
        }

        private void CreateRootPage()
        {
            _state.Depth++;
            _state.BranchPages++;

            ref var state = ref _internalCursor._stk[_internalCursor._pos];

            // we'll copy the current page and reuse it, to avoid changing the root page number
            var page = _llt.AllocatePage(1);
            
            long cpy = page.PageNumber;
            Memory.Copy(page.Pointer, state.Page.Pointer, Constants.Storage.PageSize);
            page.PageNumber = cpy;

            Memory.Set(state.Page.DataPointer, 0, Constants.Storage.PageSize - PageHeader.SizeOf);
            state.Header->PageFlags = CompactPageFlags.Branch;
            state.Header->Lower =  PageHeader.SizeOf + sizeof(ushort);
            state.Header->FreeSpace = Constants.Storage.PageSize - (PageHeader.SizeOf );

            var encoder = new Encoder();
            encoder.ZigZagEncode(cpy);
            var size = 1 + encoder.Length;

            state.Header->Upper = (ushort)(Constants.Storage.PageSize - size);
            state.Header->FreeSpace -= (ushort)(size + sizeof(ushort));

            state.EntriesOffsets[0] = state.Header->Upper;
            byte* entryPos = state.Page.Pointer + state.Header->Upper;
            *entryPos++ = 0; // zero len key
            Memory.Copy(entryPos, encoder.Buffer, encoder.Length);

            Debug.Assert(state.Header->FreeSpace == (state.Header->Upper - state.Header->Lower));

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
            if (_internalCursor._len + 1 >= _internalCursor._stk.Length)// should never happen
                Array.Resize(ref _internalCursor._stk, _internalCursor._stk.Length * 2); // but let's handle it
            Array.Copy(_internalCursor._stk, _internalCursor._pos + 1, _internalCursor._stk, _internalCursor._pos + 2, _internalCursor._len - (_internalCursor._pos + 1));
            _internalCursor._len++;
            _internalCursor._stk[_internalCursor._pos + 1] = newPageState;
            _internalCursor._pos++;
        }

        private void DefragPage()
        {                     
            ref var state = ref _internalCursor._stk[_internalCursor._pos];

            using (_llt.Environment.GetTemporaryPage(_llt, out var tmp))
            {
                Memory.Copy(tmp.TempPagePointer, state.Page.Pointer, Constants.Storage.PageSize);

                var tmpHeader = (CompactPageHeader*)tmp.TempPagePointer;
                tmpHeader->Upper = Constants.Storage.PageSize;

                var entriesOffsets = new Span<ushort>(tmp.TempPagePointer + PageHeader.SizeOf, state.Header->NumberOfEntries); 
                for (int i = 0; i < state.Header->NumberOfEntries; i++)
                {
                    GetEntryBuffer(state.Page, state.EntriesOffsets[i], out var b, out var len);

                    Debug.Assert((tmpHeader->Upper - len) > 0);
                    tmpHeader->Upper -= (ushort)len;
                    
                    // Note: FreeSpace doesn't change here
                    Memory.Copy(tmp.TempPagePointer + tmpHeader->Upper, b, len);
                    entriesOffsets[i] = tmpHeader->Upper;
                }
                // We have consolidated everything therefore we need to update the new free space value.
                tmpHeader->FreeSpace = (ushort)(tmpHeader->Upper - tmpHeader->Lower);
                
                Memory.Copy(state.Page.Pointer, tmp.TempPagePointer, Constants.Storage.PageSize);
                Memory.Set(state.Page.Pointer + tmpHeader->Lower, 0, tmpHeader->Upper - tmpHeader->Lower);

                Debug.Assert(state.Header->FreeSpace == (state.Header->Upper - state.Header->Lower));
            }
        }

        private EncodedKey FindPageFor(ReadOnlySpan<byte> key, ref IteratorCursorState cstate)
        {
            cstate._pos = -1;
            cstate._len = 0;
            PushPage(_state.RootPage, ref cstate);

            ref var state = ref cstate._stk[cstate._pos];
            var encodedKey = EncodedKey.Get(key, this, state.Header->DictionaryId);

            while (state.Header->PageFlags.HasFlag(CompactPageFlags.Branch))
            {
                encodedKey = SearchPageAndPushNext(encodedKey, ref cstate);
                state = ref cstate._stk[cstate._pos];
            }

            SearchInCurrentPage(encodedKey, ref cstate._stk[cstate._pos]);
            return encodedKey;
        }

        private EncodedKey SearchPageAndPushNext(in EncodedKey key, ref IteratorCursorState cstate)
        {
            SearchInCurrentPage(key, ref cstate._stk[cstate._pos]);

            ref var state = ref cstate._stk[cstate._pos];
            if (state.LastSearchPosition < 0)
                state.LastSearchPosition = ~state.LastSearchPosition;
            if (state.LastMatch != 0 && state.LastSearchPosition > 0)
                state.LastSearchPosition--; // went too far

            int actualPos = Math.Min(state.Header->NumberOfEntries - 1, state.LastSearchPosition);
            var nextPage = GetValue(ref state, actualPos);

            PushPage(nextPage, ref cstate);

            return EncodedKey.Get(key, this, cstate._stk[cstate._pos].Header->DictionaryId);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PopPage(ref IteratorCursorState cstate)
        {
            cstate._stk[cstate._pos--] = default;
            cstate._len--;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PushPage(long nextPage, ref IteratorCursorState cstate)
        {
            if (cstate._pos + 1 >= cstate._stk.Length) //  should never actually happen
                Array.Resize(ref cstate._stk, cstate._stk.Length * 2); // but let's be safe

            Page page = _llt.GetPage(nextPage);
            cstate._stk[++cstate._pos] = new CursorState { Page = page, };
            cstate._len++;
            
            // For the creation of the dictionary if it doesnt exist.
            // TODO: I dont even think this is needed at all. 
            CompactPageHeader* pageHeader = (CompactPageHeader*)page.Pointer;
            var _ = GetEncodingDictionary(pageHeader->DictionaryId);
        }

        private PersistentDictionary CreateEncodingDictionary(long dictionaryId)
        {
            // TODO: Given that dictionaries are static when created, we can actually have a cache of them that could survive transactional work. 
            return new PersistentDictionary(_llt.GetPage(dictionaryId));
        }

        private PersistentDictionary GetEncodingDictionary(long dictionaryId)
        {
            if (!_dictionaries.TryGetValue(dictionaryId, out var dictionary))
            {
                dictionary = CreateEncodingDictionary(dictionaryId);
                _dictionaries[dictionaryId] = dictionary;
            }

            return dictionary;
        }


        private static ReadOnlySpan<byte> GetEncodedKey(Page page, ushort entryOffset)
        {
            var entryPos = page.Pointer + entryOffset;
            var keyLen = Encoder.Decode7Bits(entryPos, out var lenOfKeyLen);
            return new ReadOnlySpan<byte>(page.Pointer + entryOffset + lenOfKeyLen,  (int)keyLen);
        }

        private static long GetValue(ref CursorState state, int pos)
        {
            GetValuePointer(ref state, pos, out var p);
            return Encoder.ZigZagDecode(p, out _);
        }

        private static void GetValuePointer(ref CursorState state, int pos, out byte* p)
        {
            ushort entryOffset = state.EntriesOffsets[pos];
            p = state.Page.Pointer + entryOffset;
            var keyLen = (int)Encoder.Decode7Bits(p, out var lenKeyLen);
            p += keyLen + lenKeyLen;
        }

        internal static int GetEncodedEntry(Page page, ushort entryOffset, out Span<byte> key, out long value)
        {
            if(entryOffset < PageHeader.SizeOf)
                throw new ArgumentOutOfRangeException();
            byte* entryPos = page.Pointer + entryOffset;
            var keyLen = (int)Encoder.Decode7Bits(entryPos, out var lenKeyLen);
            key = new Span<byte>(entryPos + lenKeyLen, keyLen);
            entryPos += keyLen + lenKeyLen;
            value = Encoder.ZigZagDecode(entryPos, out var valLen);
            entryPos += valLen;
            return (int)(entryPos - page.Pointer - entryOffset);
        }

        internal static void GetEntry(CompactTree tree, Page page, ushort entriesOffset, out Span<byte> key, out long value)
        {
            var result = GetEncodedEntry(page, entriesOffset, out key, out value);
            EncodedKey encodedKey = EncodedKey.From(key, tree, ((CompactPageHeader*)page.Pointer)->DictionaryId);

            tree.Llt.Allocator.Allocate(encodedKey.Key.Length, out var output);
            
            encodedKey.Key.CopyTo(output.ToSpan());
            
            var outputSpan = output.ToSpan();
            key = output[^1] == 0 ? outputSpan : outputSpan.Slice(0, outputSpan.Length - 1);
        }

        private static void GetEntryBuffer(Page page, ushort entryOffset, out byte* b, out int len)
        {
            byte* entryPos = b = page.Pointer + entryOffset;
            var keyLen = (int)Encoder.Decode7Bits(entryPos, out var lenKeyLen);
            Encoder.ZigZagDecode(entryPos + keyLen + lenKeyLen, out var valLen);
            len = lenKeyLen + keyLen + valLen;
        }

        private void SearchInCurrentPage(in EncodedKey key, ref CursorState state)
        {
            // Ensure that the key has already been 'updated' this is internal and shouldn't check explicitly that.
            // It is the responsibility of the caller to ensure that is the case. 
            Debug.Assert(key.Dictionary == state.Header->DictionaryId);

            var encodedKey = key.Encoded;

            int high = state.Header->NumberOfEntries - 1, low = 0;
            int match = -1;
            int mid = 0;
            while (low <= high)
            {
                mid = (high + low) / 2;
                var cur = GetEncodedKey(state.Page, state.EntriesOffsets[mid]);

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

        private static int DictionaryOrder(ReadOnlySpan<byte> s1, ReadOnlySpan<byte> s2)
        {
            // Bed - Tree: An All-Purpose Index Structure for String Similarity Search Based on Edit Distance
            // https://event.cwi.nl/SIGMOD-RWE/2010/12-16bf4c/paper.pdf

            //  Intuitively, such a sorting counts the total number of strings with length smaller than |s|
            //  plus the number of string with length equal to |s| preceding s in dictionary order.

            int len1 = s1.Length;
            int len2 = s2.Length;

            if (len1 == 0 && len2 == 0)
                return 0;

            if (len1 == 0)
                return -1;

            if (len2 == 0)
                return 1;

            //  Given two strings si and sj, it is sufficient to find the most significant position p where the two string differ
            int minLength = len1 < len2 ? len1 : len2;

            // If π(si[p]) < π(sj[p]), we can assert that si precedes sj in dictionary order φd, and viceversa.
            int i;
            for (i = 0; i < minLength; i++)
            {
                if (s1[i] < s2[i])
                    return -1;
                else if (s2[i] < s1[i])
                    return 1;
            }

            if (len1 < len2)
                return -1;

            return 1;
        }

        private void FuzzySearchInCurrentPage(in EncodedKey key, ref CursorState state)
        {
            // Ensure that the key has already been 'updated' this is internal and shouldn't check explicitly that.
            // It is the responsibility of the caller to ensure that is the case. 
            Debug.Assert(key.Dictionary == state.Header->DictionaryId);

            var encodedKey = key.Encoded;

            int high = state.Header->NumberOfEntries - 1, low = 0;
            int match = -1;
            int mid = 0;
            while (low <= high)
            {
                mid = (high + low) / 2;
                var cur = GetEncodedKey(state.Page, state.EntriesOffsets[mid]);

                match = DictionaryOrder(key.Key, cur);

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

        private EncodedKey FuzzySearchPageAndPushNext(in EncodedKey key, ref IteratorCursorState cstate)
        {
            FuzzySearchInCurrentPage(key, ref cstate._stk[cstate._pos]);

            ref var state = ref cstate._stk[cstate._pos];
            if (state.LastSearchPosition < 0)
                state.LastSearchPosition = ~state.LastSearchPosition;
            if (state.LastMatch != 0 && state.LastSearchPosition > 0)
                state.LastSearchPosition--; // went too far

            int actualPos = Math.Min(state.Header->NumberOfEntries - 1, state.LastSearchPosition);
            var nextPage = GetValue(ref state, actualPos);

            PushPage(nextPage, ref cstate);

            return EncodedKey.Get(key, this, cstate._stk[cstate._pos].Header->DictionaryId);
        }

        private EncodedKey FuzzyFindPageFor(ReadOnlySpan<byte> key, ref IteratorCursorState cstate)
        {
            // Algorithm 2: Find Node

            cstate._pos = -1;
            cstate._len = 0;
            PushPage(_state.RootPage, ref cstate);

            ref var state = ref cstate._stk[cstate._pos];
            var encodedKey = EncodedKey.Get(key, this, state.Header->DictionaryId);

            while (state.Header->PageFlags.HasFlag(CompactPageFlags.Branch))
            {
                encodedKey = FuzzySearchPageAndPushNext(encodedKey, ref cstate);
                state = ref cstate._stk[cstate._pos];
            }
                        
            // if N is the leaf node then
            //    Return N
            state.LastMatch = 1;
            state.LastSearchPosition = 0;       
            return encodedKey;
        }

    }
}
