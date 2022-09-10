using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.BTrees;
using Voron.Debugging;
using Voron.Global;
using Voron.Impl;
using Voron.Exceptions;
using Sparrow.Server.Compression;

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
        
        internal CompactTreeState State => _state;
        internal LowLevelTransaction Llt => _llt;

        public readonly ref struct EncodedKey
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

                // guard against a scenario where compression actually increase the size of the key beyond our limits
                if (encodedKeySpan.Length > Constants.CompactTree.MaximumKeySize)
                    throw new ArgumentOutOfRangeException(nameof(key), Encoding.UTF8.GetString(key),$"key (both encoded and plain) must be less than {Constants.CompactTree.MaximumKeySize} bytes in size, but the **encoded** key was larger than that!");

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

        internal struct CursorState
        {
            public Page Page;
            public int LastMatch;
            public int LastSearchPosition;
            public CompactPageHeader* Header => (CompactPageHeader*)Page.Pointer;
            
            public Span<ushort> EntriesOffsets => new Span<ushort>(Page.Pointer+ PageHeader.SizeOf, Header->NumberOfEntries);
            public ushort* EntriesOffsetsPtr => (ushort*)(Page.Pointer + PageHeader.SizeOf);
            
            public int ComputeFreeSpace()
            {
                var usedSpace = PageHeader.SizeOf + sizeof(ushort) * Header->NumberOfEntries;
                for (int i = 0; i < Header->NumberOfEntries; i++)
                {
                    GetEntryBuffer(Page, EntriesOffsets[i], out _, out var len);
                    Debug.Assert(len < Constants.CompactTree.MaximumKeySize);
                    usedSpace += len;
                }

                int computedFreeSpace = (Constants.Storage.PageSize - usedSpace);
                Debug.Assert(computedFreeSpace >= 0);
                return computedFreeSpace;
            }
            public string DumpPageDebug(CompactTree tree)
            {
                var dictionary = tree.GetEncodingDictionary(Header->DictionaryId);

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
                // We also need to limit the amount of time that we are allowed to scan trying to find a better dictionary
                TryImproveDictionaryByRandomlyScanning(Math.Min(_state.NumberOfEntries / 10, 1_000));
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
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(string key, out long value)
        {
            return TryGetValue(key, out value, out var _);
        }


        public bool TryGetValue(string key, out long value, out EncodedKey encodedKey)
        {
            using var _ = Slice.From(_llt.Allocator, key, out var slice);
            var span = slice.AsReadOnlySpan();
            return TryGetValue(span, out value, out encodedKey);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(ReadOnlySpan<byte> key, out long value)
        {
            return TryGetValue(key, out value, out var _);
        }
        
        public bool TryGetValue(ReadOnlySpan<byte> key, out long value, out EncodedKey encodedKey)
        {
            encodedKey = FindPageFor(key, ref _internalCursor);

            return ReturnValue(encodedKey, ref _internalCursor._stk[_internalCursor._pos], out value);
        }

        private static bool ReturnValue(in EncodedKey encodedKey, ref CursorState state, out long value)
        {
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

        public void InitializeStateForTryGetNextValue()
        {
            _internalCursor._pos = -1;
            _internalCursor._len = 0;
            PushPage(_state.RootPage, ref _internalCursor);
            ref var state = ref _internalCursor._stk[_internalCursor._pos];
            state.LastSearchPosition = 0;
        }
        
        public bool TryGetNextValue(ReadOnlySpan<byte> key, out long value, out EncodedKey encodedKey)
        {
            ref var state = ref _internalCursor._stk[_internalCursor._pos];
            if (state.Header->PageFlags == CompactPageFlags.Branch)
            {
                // the *previous* search didn't find a value, we are on a branch page that may
                // be correct or not, try first to search *down*
                encodedKey = EncodedKey.Get(key, this, state.Header->DictionaryId);
                encodedKey = FindPageFor(ref _internalCursor, ref state, encodedKey);
                state = ref _internalCursor._stk[_internalCursor._pos];

                if (state.LastMatch == 0) // found it
                    return ReturnValue(encodedKey, ref state, out value);
                // did *not* find it, but we are somewhere on the tree that is ensured
                // to be at the key location *or before it*, so we can now start scanning *up*
            }
            Debug.Assert(state.Header->PageFlags == CompactPageFlags.Leaf, $"Got {state.Header->PageFlags} flag instead of {nameof(CompactPageFlags.Leaf)}");
            
            encodedKey = EncodedKey.Get(key, this, state.Header->DictionaryId);

            SearchInCurrentPage(encodedKey, ref state);
            if (state.LastSearchPosition  >= 0) // found it, yeah!
            {
                value = GetValue(ref state, state.LastSearchPosition);
                return true;
            }

            var pos = ~state.LastSearchPosition;
            var shouldBeInCurrentPage = pos < state.Header->NumberOfEntries;

            if (shouldBeInCurrentPage)
            {
                var nextEntry = GetEncodedKey(state.Page, state.EntriesOffsets[pos]);
                var match = encodedKey.Encoded.SequenceCompareTo(nextEntry);
                shouldBeInCurrentPage = match < 0;
            }

            if (shouldBeInCurrentPage == false)
            {
                // if this isn't in this page, it may be in the _next_ one, but we 
                // now need to check the parent page to see that
                shouldBeInCurrentPage = true;
                for (int i = _internalCursor._pos - 1; i >= 0; i--)
                {
                    ref var cur = ref _internalCursor._stk[i];
                    if (cur.LastSearchPosition + 1 >= cur.Header->NumberOfEntries)
                        continue;

                    var nextEntry = GetEncodedKey(cur.Page, cur.EntriesOffsets[cur.LastSearchPosition + 1]);
                    var currentKeyInPageDictionary = EncodedKey.Get(encodedKey, this, cur.Header->DictionaryId);
                    var match = currentKeyInPageDictionary.Encoded.SequenceCompareTo(nextEntry);
                    if (match < 0)
                        continue;

                    shouldBeInCurrentPage = false;
                    break;
                }
            }

            if (shouldBeInCurrentPage)
            {
                // we didn't find the key, but we found a _greater_ key in the page
                // therefore, we don't have it (we know the previous key was in this page
                // so if there is a greater key in this page, we didn't find it
                value = default;
                return false;
            }

            while (_internalCursor._pos > 0)
            {
                PopPage(ref _internalCursor);
                state = ref _internalCursor._stk[_internalCursor._pos];
                var previousSearchPosition = state.LastSearchPosition;

                encodedKey = EncodedKey.Get(encodedKey, this, _internalCursor._stk[_internalCursor._pos].Header->DictionaryId);
                SearchInCurrentPage(encodedKey, ref state);

                if (state.LastSearchPosition < 0)
                    state.LastSearchPosition = ~state.LastSearchPosition;
        
                // is this points to a different page, just search there normally
                if (state.LastSearchPosition > previousSearchPosition && state.LastSearchPosition < state.Header->NumberOfEntries )
                {
                    encodedKey = FindPageFor(ref _internalCursor, ref state, encodedKey);
                    return ReturnValue(encodedKey, ref _internalCursor._stk[_internalCursor._pos], out value);
                }
            }
            
            // if we go to here, we are at the root, so operate normally
            return TryGetValue(key, out value, out encodedKey);
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

            var keyLen = VariableSizeEncoding.Read<int>(entry, out var lenOfKeyLen);
            entry += keyLen + lenOfKeyLen;
            oldValue = ZigZagEncoding.Decode<long>(entry, out var valLen);

            var totalEntrySize = lenOfKeyLen + keyLen + valLen;
            state.Header->FreeSpace += (ushort)(sizeof(ushort) + totalEntrySize);
            state.Header->Lower -= sizeof(short); // the upper will be fixed on defrag
            
            Debug.Assert(state.Header->Upper - state.Header->Lower >= 0);
            Debug.Assert(state.Header->FreeSpace <= Constants.Storage.PageSize - PageHeader.SizeOf);

            entriesOffsets[(state.LastSearchPosition + 1)..].CopyTo(entriesOffsets[state.LastSearchPosition..]);
            
            if (state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
            {
                _state.NumberOfEntries--;
            }

            if (allowRecurse &&
                _internalCursor._pos > 0 && // nothing to do for a single leaf node
                state.Header->FreeSpace > Constants.Storage.PageSize / 2)
            {
                // We check if we need to run defrag by seeing if there is a significant difference between the free space in the page
                // and the actual available space (should have at least 1KB to recover before we run)
                // It is in our best interest to defrag the receiving page to avoid having to  try again and again without achieving any gains.
                if (state.Header->Upper - state.Header->Lower < state.Header->FreeSpace - Constants.Storage.PageSize / 8)
                    DefragPage(_llt, ref state);

                if (MaybeMergeEntries(ref state))
                    InitializeStateForTryGetNextValue(); // we change the structure of the tree, so we can't reuse 
            }

            VerifySizeOf(ref state);
            return true;
        }                

        private bool MaybeMergeEntries(ref CursorState destinationState)
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
                    return true;
                }

                return false;
            }

            var siblingPage = GetValue(ref parent, parent.LastSearchPosition + 1);
            sourceState = new CursorState
            {
                Page = _llt.ModifyPage(siblingPage)
            };

            if (sourceState.Header->PageFlags != destinationState.Header->PageFlags)
                return false; // cannot merge leaf & branch pages

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
            bool reEncode = sourceHeader->DictionaryId != destinationHeader->DictionaryId;

            int sourceMovedLength = 0;
            int sourceKeysCopied = 0;
            {
                for (; sourceKeysCopied < sourceHeader->NumberOfEntries; sourceKeysCopied++)
                {
                    var copied = reEncode
                        ? MoveEntryWithReEncoding(decodeBuffer, encodeBuffer, ref destinationState, entries)
                        : MoveEntryAsIs(ref destinationState, entries);
                    if (copied == false)
                        break;
                }
            }

            if (sourceKeysCopied == 0)
                return false;

            Memory.Move(sourcePage.Pointer + PageHeader.SizeOf,
                        sourcePage.Pointer + PageHeader.SizeOf + (sourceKeysCopied * sizeof(ushort)),
                        (sourceHeader->NumberOfEntries - sourceKeysCopied) * sizeof(ushort));
            
            // We update the entries offsets on the source page, now that we have moved the entries.
            var oldLower = sourceHeader->Lower;
            sourceHeader->Lower -= (ushort)(sourceKeysCopied * sizeof(ushort));            
            if (sourceHeader->NumberOfEntries == 0) // emptied the sibling entries
            {
                parent.LastSearchPosition++;
                FreePageFor(ref destinationState, ref sourceState);
                return true;
            }

            sourceHeader->FreeSpace += (ushort)(sourceMovedLength + (sourceKeysCopied * sizeof(ushort)));
            Memory.Set(sourcePage.Pointer + sourceHeader->Lower, 0, (oldLower - sourceHeader->Lower));

            // now re-wire the new splitted page key
            var newEncodedKey = GetEncodedKey(sourcePage, sourceState.EntriesOffsets[0]);
            var newKey = EncodedKey.From(newEncodedKey, this, sourceHeader->DictionaryId);

            PopPage(ref _internalCursor);
            
            // we aren't _really_ removing, so preventing merging of parents
            RemoveFromPage(allowRecurse: false, parent.LastSearchPosition + 1);

            // Ensure that we got the right key to search. 
            newKey = EncodedKey.Get(newKey, this, _internalCursor._stk[_internalCursor._pos].Header->DictionaryId);
            SearchInCurrentPage(newKey, ref _internalCursor._stk[_internalCursor._pos]); // positions changed, re-search
            AddToPage(newKey, siblingPage);
            return true;

            [SkipLocalsInit]
            bool MoveEntryWithReEncoding(Span<byte> decodeBuffer, Span<byte> encodeBuffer, ref CursorState destinationState, Span<ushort> entries)
            {
                // PERF: This method is marked SkipLocalInit because we want to avoid initialize these values
                // as we are going to be writing them anyways.
                byte* valueEncodingBuffer = stackalloc byte[16];
                byte* keyEncodingBuffer = stackalloc byte[16];

                // We get the encoded key and value from the sibling page
                var sourceEntrySize = GetEncodedEntry(sourcePage, sourceState.EntriesOffsets[sourceKeysCopied], out var encodedKey, out var val);
                
                // If they have a different dictionary, we need to re-encode the entry with the new dictionary.
                if (encodedKey.Length != 0)
                {
                    var decodedKey = decodeBuffer;
                    srcDictionary.Decode(encodedKey, ref decodedKey);

                    encodedKey = encodeBuffer;
                    destDictionary.Encode(decodedKey, ref encodedKey);
                }

                // We encode the length of the key and the value with variable length in order to store them later. 
                int valueLength = ZigZagEncoding.Encode(valueEncodingBuffer, val);
                int keySizeLength = VariableSizeEncoding.Write(keyEncodingBuffer, encodedKey.Length);

                // If we dont have enough free space in the receiving page, we move on. 
                var requiredSize = encodedKey.Length + keySizeLength + valueLength;
                if (requiredSize + sizeof(ushort) > destinationState.Header->Upper - destinationState.Header->Lower)
                    return false; // done moving entries

                sourceMovedLength += sourceEntrySize;

                // We will update the entries offsets in the receiving page.
                destinationHeader->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
                destinationHeader->Upper -= (ushort)requiredSize;
                destinationHeader->Lower += sizeof(ushort);
                entries[sourceKeysCopied] = destinationHeader->Upper;
                
                // We copy the actual entry <key_size, key, value> to the receiving page.
                var entryPos = destinationPage.Pointer + destinationHeader->Upper;                
                Memory.Copy(entryPos, keyEncodingBuffer, keySizeLength);
                entryPos += keySizeLength;
                encodedKey.CopyTo(new Span<byte>(entryPos, (int)(destinationPage.Pointer + Constants.Storage.PageSize - entryPos)));
                entryPos += encodedKey.Length;
                Memory.Copy(entryPos, valueEncodingBuffer, valueLength);                                                                

                Debug.Assert(destinationHeader->Upper >= destinationHeader->Lower);

                return true;
            }
      
            bool MoveEntryAsIs(ref CursorState destinationState, Span<ushort> entries)
            {
                // We get the encoded key and value from the sibling page
                var entry = GetEncodedEntryBuffer(sourcePage, sourceState.EntriesOffsets[sourceKeysCopied]);
                
                // If we dont have enough free space in the receiving page, we move on. 
                var requiredSize = entry.Length;
                if (requiredSize + sizeof(ushort) > destinationState.Header->Upper - destinationState.Header->Lower)
                    return false; // done moving entries

                sourceMovedLength += entry.Length;
                // We will update the entries offsets in the receiving page.
                destinationHeader->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
                destinationHeader->Upper -= (ushort)requiredSize;
                destinationHeader->Lower += sizeof(ushort);
                entries[sourceKeysCopied] = destinationHeader->Upper;
                
                // We copy the actual entry <key_size, key, value> to the receiving page.
                entry.CopyTo(destinationPage.AsSpan().Slice(destinationHeader->Upper));

                Debug.Assert(destinationHeader->Upper >= destinationHeader->Lower);
                return true;
            }
      
         }

        private void FreePageFor(ref CursorState stateToKeep, ref CursorState stateToDelete)
        {
            ref var parent = ref _internalCursor._stk[_internalCursor._pos - 1];
            DecrementPageNumbers(ref stateToDelete);
          
            if (parent.Header->NumberOfEntries == 2)
            {
                // let's reduce the height of the tree entirely...
                DecrementPageNumbers(ref parent);

                var parentPageNumber = parent.Page.PageNumber;
                Memory.Copy(parent.Page.Pointer, stateToKeep.Page.Pointer, Constants.Storage.PageSize);
                parent.Page.PageNumber = parentPageNumber; // we overwrote it...

                _llt.FreePage(stateToDelete.Page.PageNumber);
                _llt.FreePage(stateToKeep.Page.PageNumber);
                var copy = stateToKeep; // we are about to clear this value, but we need to set the search location here
                PopPage(ref _internalCursor);
                _internalCursor._stk[_internalCursor._pos].LastMatch = copy.LastMatch;
                _internalCursor._stk[_internalCursor._pos].LastMatch = copy.LastSearchPosition;
            }
            else
            {
                _llt.FreePage(stateToDelete.Page.PageNumber);
                PopPage(ref _internalCursor);
                RemoveFromPage(allowRecurse: true, parent.LastSearchPosition);
            }
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


        private void AssertValueAndKeySize(ReadOnlySpan<byte> key, long value)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "Only positive values are allowed");
            if (key.Length > Constants.CompactTree.MaximumKeySize)
                throw new ArgumentOutOfRangeException(nameof(key), Encoding.UTF8.GetString(key),$"key must be less than {Constants.CompactTree.MaximumKeySize} bytes in size");
            if(key.Length <= 0)
                throw new ArgumentOutOfRangeException(nameof(key), Encoding.UTF8.GetString(key), "key must be at least 1 byte");
        }
        
        public void Add(string key, long value)
        {
            using var _ = Slice.From(_llt.Allocator, key, out var slice);
            var span = slice.AsReadOnlySpan();
            Add(span, value);
        }

        public void Add(ReadOnlySpan<byte> key, long value)
        {
            AssertValueAndKeySize(key, value);
            
            var encodedKey = FindPageFor(key, ref _internalCursor);
            AddToPage(encodedKey, value);
        }
        
        public void Add(ReadOnlySpan<byte> key, long value, EncodedKey encodedKey)
        {
            AssertValueAndKeySize(key, value);
            // this overload assumes that a previous call to TryGetValue (where you go the encodedKey
            // already placed us in the right place for the value)
            Debug.Assert(_internalCursor._stk[_internalCursor._pos].Header->PageFlags == CompactPageFlags.Leaf,
                $"Got {_internalCursor._stk[_internalCursor._pos].Header->PageFlags} flag instead of {nameof(CompactPageFlags.Leaf)}");

            AddToPage(encodedKey, value);
        }

        [SkipLocalsInit]
        private void AddToPage(EncodedKey key, long value)
        {
            ref var state = ref _internalCursor._stk[_internalCursor._pos];

            // Ensure that the key has already been 'updated' this is internal and shouldn't check explicitly that.
            // It is the responsibility of the caller to ensure that is the case. 
            Debug.Assert(key.Dictionary == state.Header->DictionaryId);
            
            state.Page = _llt.ModifyPage(state.Page.PageNumber);

            var valueBufferPtr = stackalloc byte[10];
            int valueBufferLength = ZigZagEncoding.Encode(valueBufferPtr, value);

            if (state.LastSearchPosition >= 0) // update
            {
                GetValuePointer(ref state, state.LastSearchPosition, out var b);
                ZigZagEncoding.Decode<long>(b, out var len);

                if (len == valueBufferLength)
                {
                    Debug.Assert(valueBufferLength <= sizeof(long));
                    Unsafe.CopyBlockUnaligned(b, valueBufferPtr, (uint)valueBufferLength);
                    return;
                }

                // remove the entry, we'll need to add it as new
                int entriesCount = state.Header->NumberOfEntries;
                ushort* stateEntriesOffsetsPtr = state.EntriesOffsetsPtr;
                GetEntryBuffer(state.Page, state.EntriesOffsets[state.LastSearchPosition], out _, out var totalEntrySize);
                for (int i = state.LastSearchPosition; i < entriesCount - 1; i++)
                {
                    stateEntriesOffsetsPtr[i] = stateEntriesOffsetsPtr[i + 1];
                }

                state.Header->Lower -= sizeof(short);
                state.Header->FreeSpace += (ushort)(sizeof(short) + totalEntrySize);
                if (state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
                    _state.NumberOfEntries--; // we aren't counting branch entries
            }
            else
            {
                state.LastSearchPosition = ~state.LastSearchPosition;
            }

            var keySizeBufferPtr = stackalloc byte[10];
            var keySizeBuffer = new Span<byte>(keySizeBufferPtr, 10);
            int keySizeLength = VariableSizeEncoding.Write(keySizeBuffer, key.Encoded.Length);

            var requiredSize = key.Encoded.Length + keySizeLength + valueBufferLength;
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
                        Debug.Assert(state.Header->FreeSpace >= (state.Header->Upper - state.Header->Lower));
  
                        // Since the recompressing has changed the topology of the entire page, we need to reencode the key
                        // to move forward. 
                        key = EncodedKey.Get(key, this, state.Header->DictionaryId);

                        // We need to recompute this because it will change.
                        keySizeLength = VariableSizeEncoding.Write(keySizeBuffer, key.Encoded.Length);
                        requiredSize = key.Encoded.Length + keySizeLength + valueBufferLength;
                        
                        // It may happen that between the more effective dictionary and the reclaimed space we have enough
                        // to avoid the split. 
                        if (state.Header->Upper - state.Header->Lower >= requiredSize + sizeof(short))
                            splitAnyways = false;
                    }
                }
                else
                {
                    // If we are not recompressing, but we still have enough free space available we will go for reclaiming space
                    // by rearranging unused space.
                    if (state.Header->FreeSpace >= requiredSize + sizeof(short) &&
                        // at the same time, we need to avoid spending too much time doing de-frags
                        // so we'll only do that when we have at least 1KB of free space to recover
                        state.Header->FreeSpace > Constants.Storage.PageSize / 8)
                    {
                        DefragPage(_llt, ref state);
                        splitAnyways = state.Header->Upper - state.Header->Lower < requiredSize + sizeof(short);
                    }
                }

                if (splitAnyways)
                {
                    // DebugStuff.RenderAndShow(this);
                    SplitPage(key, value);
                    return;
                    // DebugStuff.RenderAndShow(this);
                }
            }

            AddEntryToPage(key, state, requiredSize, keySizeBufferPtr, keySizeLength, valueBufferPtr, valueBufferLength);
        }

        private void AddEntryToPage(EncodedKey key, CursorState state, int requiredSize,
            byte* keySizeBufferPtr, int keySizeLength, byte* valueBufferPtr, int valueBufferLength)
        {
            // Ensure that the key has already been 'updated' this is internal and shouldn't check explicitly that.
            // It is the responsibility of the method to ensure that is the case. 
            Debug.Assert(key.Dictionary == state.Header->DictionaryId);

            state.Header->Lower += sizeof(short);
            var newEntriesOffsets = state.EntriesOffsets;
            var newNumberOfEntries = state.Header->NumberOfEntries;

            ushort* newEntriesOffsetsPtr = state.EntriesOffsetsPtr;
            for (int i = newNumberOfEntries - 1; i >= state.LastSearchPosition; i--)
                newEntriesOffsetsPtr[i] = newEntriesOffsetsPtr[i - 1];

            if (state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
                _state.NumberOfEntries++; // we aren't counting branch entries

            Debug.Assert(state.Header->FreeSpace >= requiredSize + sizeof(ushort));

            state.Header->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
            state.Header->Upper -= (ushort)requiredSize;

            byte* writePos = state.Page.Pointer + state.Header->Upper;
            Unsafe.CopyBlockUnaligned(writePos, keySizeBufferPtr, (uint)keySizeLength);
            writePos += keySizeLength;
            key.Encoded.CopyTo(new Span<byte>(writePos, key.Encoded.Length));
            writePos += key.Encoded.Length;
            Unsafe.CopyBlockUnaligned(writePos, valueBufferPtr, (uint)valueBufferLength);
            newEntriesOffsets[state.LastSearchPosition] = state.Header->Upper;
            VerifySizeOf(ref state);
        }

        private EncodedKey SplitPage(EncodedKey currentCauseForSplit, long value)
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
            Debug.Assert(currentCauseForSplit.Dictionary == state.Header->DictionaryId);

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
            var splitKey = SplitPageEncodedEntries(currentCauseForSplit, page, header, value, ref state);

            PopPage(ref _internalCursor); // add to parent
            splitKey = EncodedKey.Get(splitKey, this, _internalCursor._stk[_internalCursor._pos].Header->DictionaryId);

            SearchInCurrentPage(splitKey, ref _internalCursor._stk[_internalCursor._pos]);
            AddToPage(splitKey, page.PageNumber);

            if (_internalCursor._stk[_internalCursor._pos].Header->PageFlags == CompactPageFlags.Leaf)
            {
                // we change the structure of the tree, so we can't reuse the state
                // but we can only do that as the _last_ part of the operation, otherwise
                // recursive split page will give bad results
                InitializeStateForTryGetNextValue(); 
            }

            VerifySizeOf(ref state);
            return currentCauseForSplit;
        }

        private EncodedKey SplitPageEncodedEntries(EncodedKey causeForSplit, Page page, CompactPageHeader* header, long value, ref CursorState state)
        {
            var valueBufferPtr = stackalloc byte[10];
            int valueBufferLength = ZigZagEncoding.Encode(valueBufferPtr, value);
            var keySizeBufferPtr = stackalloc byte[10];
            var keySizeBuffer = new Span<byte>(keySizeBufferPtr, 10);
            int keySizeLength = VariableSizeEncoding.Write(keySizeBuffer, causeForSplit.Encoded.Length);
            var requiredSize = causeForSplit.Encoded.Length + keySizeLength + valueBufferLength;
          
            var newPageState = new CursorState { Page = page };

            // sequential write up, no need to actually split
            int numberOfEntries = state.Header->NumberOfEntries;
            if (numberOfEntries == state.LastSearchPosition && state.LastMatch > 0)
            {
                newPageState.LastSearchPosition = 0; // add as first
                AddEntryToPage(causeForSplit, newPageState, requiredSize, keySizeBufferPtr, keySizeLength, valueBufferPtr, valueBufferLength);
                return causeForSplit;
            }

            // non sequential write, let's just split in middle
            int entriesCopied = 0;
            int sizeCopied = 0;
            ushort* offsets = newPageState.EntriesOffsetsPtr;
            int i = FindPositionToSplitPageInHalfBasedOfEntriesSize(ref state);

            for (; i < numberOfEntries; i++)
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

            DefragPage(_llt, ref state); // need to ensure that we have enough space to add the new entry in the source page
            
            var lastEntryFromPreviousPage = GetEncodedKey(state.Page, state.EntriesOffsets[state.Header->NumberOfEntries - 1]);
            ref CursorState updatedPageState = ref newPageState; // start with the new page
            if (lastEntryFromPreviousPage.SequenceCompareTo(causeForSplit.Encoded) >= 0)
            {
                // the new entry belong on the *old* page
                updatedPageState = ref state;
            }
            
            SearchInCurrentPage(causeForSplit, ref updatedPageState);
            Debug.Assert(updatedPageState.LastSearchPosition < 0, "There should be no updates here");
            updatedPageState.LastSearchPosition = ~updatedPageState.LastSearchPosition;
            Debug.Assert(updatedPageState.Header->Upper - updatedPageState.Header->Lower >= requiredSize);
            AddEntryToPage(causeForSplit, updatedPageState, requiredSize, keySizeBufferPtr, keySizeLength, valueBufferPtr, valueBufferLength);

            VerifySizeOf(ref newPageState);
            VerifySizeOf(ref state);
            
            var pageEntries = new Span<ushort>(page.Pointer + PageHeader.SizeOf, header->NumberOfEntries);
            GetEncodedEntry(page, pageEntries[0], out var splitKey, out _);

            return EncodedKey.From(splitKey, this, ((CompactPageHeader*)page.Pointer)->DictionaryId);
        }

        private static int FindPositionToSplitPageInHalfBasedOfEntriesSize(ref CursorState state)
        {
            int sizeUsed = 0;
            var halfwaySizeMark = (Constants.Storage.PageSize - state.Header->FreeSpace) / 2;
            int numberOfEntries = state.Header->NumberOfEntries;
            // here we have to guard against wildly unbalanced page structure, if the first 6 entries are 1KB each
            // and we have another 100 entries that are a byte each, if we split based on entry count alone, we'll 
            // end up unbalanced, so we compute the halfway mark based on the _size_ of the entries, not their count
            for (int i =0; i < numberOfEntries; i++)
            {
                GetEntryBuffer(state.Page, state.EntriesOffsets[i], out var b, out var len);
                sizeUsed += len;
                if (sizeUsed >= halfwaySizeMark)
                    return i;
            }
            // we should never reach here, but let's have a reasonable default
            Debug.Assert(false, "How did we reach here?");
            return numberOfEntries / 2;
        }

        public void VerifySizeOf(long page)
        {
            var state = new CursorState { Page = _llt.GetPage(page) };
            VerifySizeOf(ref state);
        }

        [Conditional("DEBUG")]
        private static void VerifySizeOf(ref CursorState p)
        {
            if (p.Header == null)
                return; // page may have been released
            var actualFreeSpace = p.ComputeFreeSpace();
            if (p.Header->FreeSpace != actualFreeSpace)
            {
                throw new InvalidOperationException("The sizes do not match! FreeSpace: " + p.Header->FreeSpace + " but actually was space: " + actualFreeSpace);
            }
        }

        

        [Conditional("DEBUG")]
        public void Render()
        {
            DebugStuff.RenderAndShow(this);
        }

        [SkipLocalsInit]
        private bool TryRecompressPage(in CursorState state)
        {
            var oldDictionary = GetEncodingDictionary(state.Header->DictionaryId);
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

            var valueBufferPtr = stackalloc byte[16];
            var keySizeBufferPtr = stackalloc byte[16];

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

                int valueLength = ZigZagEncoding.Encode(valueBufferPtr, val);
                int keySizeLength = VariableSizeEncoding.Write(keySizeBufferPtr, encodedKey.Length);

                // It may very well happen that there is no enough encoding space to upgrade the page
                // because of an slightly inefficiency at this particular page. In those cases, we wont
                // upgrade the page and just fail. 
                var requiredSize = encodedKey.Length + keySizeLength + valueLength;
                if (requiredSize + sizeof(ushort) > state.Header->FreeSpace)
                    goto Failure;

                state.Header->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
                state.Header->Lower += sizeof(ushort);
                state.Header->Upper -= (ushort)requiredSize;
                newEntries[i] = state.Header->Upper;
                var entryPos = state.Page.Pointer + state.Header->Upper;
                Memory.Copy(entryPos, keySizeBufferPtr, keySizeLength);
                entryPos += keySizeLength;
                encodedKey.CopyTo(new Span<byte>(entryPos, (int)(state.Page.Pointer + Constants.Storage.PageSize - entryPos)));
                entryPos += encodedKey.Length;
                Memory.Copy(entryPos, valueBufferPtr, valueLength);
            }

            Debug.Assert(state.Header->FreeSpace == (state.Header->Upper - state.Header->Lower));

            state.Header->DictionaryId = newDictionary.PageNumber;
            _llt.PersistentDictionariesForCompactTrees[newDictionary.PageNumber] = newDictionary;

            return true;

            Failure:
            // TODO: Probably it is best to just not allocate and copy the page afterwards if we use it. 
            Memory.Copy(state.Page.Pointer, tmp.TempPagePointer, Constants.Storage.PageSize);
            return false;
        }

        private void CreateRootPage()
        {
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

            var pageNumberBufferPtr = stackalloc byte[16];
            int pageNumberLength = ZigZagEncoding.Encode(pageNumberBufferPtr, cpy);
            var size = 1 + pageNumberLength;

            state.Header->Upper = (ushort)(Constants.Storage.PageSize - size);
            state.Header->FreeSpace -= (ushort)(size + sizeof(ushort));

            state.EntriesOffsets[0] = state.Header->Upper;
            byte* entryPos = state.Page.Pointer + state.Header->Upper;
            *entryPos++ = 0; // zero len key
            Memory.Copy(entryPos, pageNumberBufferPtr, pageNumberLength);

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

        private static void DefragPage(LowLevelTransaction llt, ref CursorState state)
        {                     
            using (llt.Environment.GetTemporaryPage(llt, out var tmp))
            {
                // Ensure we clean up the page.               
                Unsafe.InitBlock(tmp.TempPagePointer, 0, Constants.Storage.PageSize);

                // We copy just the header and start working from there.
                var tmpHeader = (CompactPageHeader*)tmp.TempPagePointer;
                *tmpHeader = *(CompactPageHeader*)state.Page.Pointer;
                                
                Debug.Assert(tmpHeader->Upper - tmpHeader->Lower >= 0);
                Debug.Assert(tmpHeader->FreeSpace <= Constants.Storage.PageSize - PageHeader.SizeOf);
                
                // We reset the data pointer                
                tmpHeader->Upper = Constants.Storage.PageSize;

                var tmpEntriesOffsets = new Span<ushort>(tmp.TempPagePointer + PageHeader.SizeOf, state.Header->NumberOfEntries);

                // For each entry in the source page, we copy it to the temporary page.
                var sourceEntriesOffsets = state.EntriesOffsets;
                for (int i = 0; i < sourceEntriesOffsets.Length; i++)
                {
                    // Retrieve the entry data from the source page
                    GetEntryBuffer(state.Page, sourceEntriesOffsets[i], out var entryBuffer, out var len);
                    Debug.Assert((tmpHeader->Upper - len) > 0);

                    ushort lowerIndex = (ushort)(tmpHeader->Upper - len);

                    // Note: Since we are just defragmentating, FreeSpace doesn't change.
                    Unsafe.CopyBlockUnaligned(tmp.TempPagePointer + lowerIndex, entryBuffer, (uint)len);

                    tmpEntriesOffsets[i] = lowerIndex;
                    tmpHeader->Upper = lowerIndex;
                }
                // We have consolidated everything therefore we need to update the new free space value.
                tmpHeader->FreeSpace = (ushort)(tmpHeader->Upper - tmpHeader->Lower);

                // We copy back the defragmented structure on the temporary page to the actual page.
                Unsafe.CopyBlockUnaligned(state.Page.Pointer, tmp.TempPagePointer, Constants.Storage.PageSize);
                Debug.Assert(state.Header->FreeSpace == (state.Header->Upper - state.Header->Lower));
            }
        }

        public List<(string, long)> AllEntriesIn(long p)
        {
            Page page = _llt.GetPage(p);
            var state = new CursorState { Page = page, };

            var results = new List<(string, long)>();
            
            for (ushort i = 0; i < state.Header->NumberOfEntries; i++)
            {
                GetEncodedEntry(page, state.EntriesOffsets[i], out var encodedKey, out var val);
                EncodedKey key = EncodedKey.From(encodedKey, this, state.Header->DictionaryId);
                results.Add((Encoding.UTF8.GetString(key.Key), val));
            }
            return results;
        }

        public List<long> AllPages()
        {
            var results = new List<long>();
            Add(_state.RootPage);
            return results;

            void Add(long p)
            {
                Page page = _llt.GetPage(p);
                var state = new CursorState { Page = page, };

                results.Add(p);
                if (state.Header->PageFlags.HasFlag(CompactPageFlags.Branch))
                {
                    for (int i = 0; i < state.Header->NumberOfEntries; i++)
                    {
                        var next = GetValue(ref state, i);
                        Add(next);
                    }
                }
            }
        }

        private EncodedKey FindPageFor(ReadOnlySpan<byte> key, ref IteratorCursorState cstate)
        {
            cstate._pos = -1;
            cstate._len = 0;
            PushPage(_state.RootPage, ref cstate);

            ref var state = ref cstate._stk[cstate._pos];
            var encodedKey = EncodedKey.Get(key, this, state.Header->DictionaryId);

            return FindPageFor(ref cstate, ref state, encodedKey);
        }

        private EncodedKey FindPageFor(ref IteratorCursorState cstate, ref CursorState state, EncodedKey encodedKey)
        {
            while (state.Header->PageFlags.HasFlag(CompactPageFlags.Branch))
            {
                encodedKey = SearchPageAndPushNext(encodedKey, ref cstate);
                state = ref cstate._stk[cstate._pos];
            }
            SearchInCurrentPage(encodedKey, ref state);
            return encodedKey;
        }

        private EncodedKey SearchPageAndPushNext(EncodedKey key, ref IteratorCursorState cstate)
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
        }

        private PersistentDictionary _lastDictionary;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PersistentDictionary GetEncodingDictionary(long dictionaryId)
        {
            PersistentDictionary GetEncodingDictionaryUnlikely()
            {
                _llt.PersistentDictionariesForCompactTrees ??= new Dictionary<long, PersistentDictionary>();
                if (_llt.PersistentDictionariesForCompactTrees.TryGetValue(dictionaryId, out var dictionary))
                {
                    _lastDictionary = dictionary;
                    return dictionary;
                }

                dictionary = new PersistentDictionary(_llt.GetPage(dictionaryId));
                _llt.PersistentDictionariesForCompactTrees[dictionaryId] = dictionary;
                _lastDictionary = dictionary;
                return dictionary;
            }

            if (_lastDictionary is not null && _lastDictionary.PageNumber == dictionaryId)
                return _lastDictionary;

            return GetEncodingDictionaryUnlikely();
        }

        private static ReadOnlySpan<byte> GetEncodedKey(Page page, ushort entryOffset)
        {
            var entryPos = page.Pointer + entryOffset;
            var keyLen = VariableSizeEncoding.Read<ushort>(entryPos, out var lenOfKeyLen);
            return new ReadOnlySpan<byte>(page.Pointer + entryOffset + lenOfKeyLen,  (int)keyLen);
        }

        private static long GetValue(ref CursorState state, int pos)
        {
            GetValuePointer(ref state, pos, out var p);
            return ZigZagEncoding.Decode<long>(p, out _);
        }

        private static void GetValuePointer(ref CursorState state, int pos, out byte* p)
        {
            ushort entryOffset = state.EntriesOffsets[pos];
            p = state.Page.Pointer + entryOffset;
            var keyLen = VariableSizeEncoding.Read<int> (p, out var lenKeyLen);
            p += keyLen + lenKeyLen;
        }

        internal static int GetEncodedEntry(Page page, ushort entryOffset, out Span<byte> key, out long value)
        {
            if(entryOffset < PageHeader.SizeOf)
                throw new ArgumentOutOfRangeException();
            byte* entryPos = page.Pointer + entryOffset;
            var keyLen = VariableSizeEncoding.Read<int>(entryPos, out var lenOfKeyLen);
            key = new Span<byte>(entryPos + lenOfKeyLen, keyLen);
            entryPos += keyLen + lenOfKeyLen;
            value = ZigZagEncoding.Decode<long>(entryPos, out var valLen);
            entryPos += valLen;
            return (int)(entryPos - page.Pointer - entryOffset);
        }

        private static void InvalidBufferContent()
        {
            throw new VoronErrorException("Invalid data found in the buffer.");
        }

        private static Span<byte> GetEncodedEntryBuffer(Page page, ushort entryOffset)
        {
            if(entryOffset < PageHeader.SizeOf)
                throw new ArgumentOutOfRangeException();

            byte* entry = page.Pointer + entryOffset;
            byte* pos = entry;
            var keyLen = VariableSizeEncoding.ReadCompact<int>(pos, out var lenOfKeyLen, out bool success);
            if (success == false)
                InvalidBufferContent();

            pos += keyLen;
            pos += lenOfKeyLen;

            VariableSizeEncoding.ReadCompact<int>(pos, out var lenOfValue, out success);
            if (success == false)
                InvalidBufferContent();

            pos += lenOfValue;

            return new Span<byte>(entry, (int)(pos - entry));
        }

        internal static bool GetEntry(CompactTree tree, Page page, ushort entriesOffset, out Span<byte> key, out long value)
        {
            GetEncodedEntry(page, entriesOffset, out key, out value);
            if (key.Length == 0)
                return false;
            
            EncodedKey encodedKey = EncodedKey.From(key, tree, ((CompactPageHeader*)page.Pointer)->DictionaryId);

            tree.Llt.Allocator.Allocate(encodedKey.Key.Length, out var output);            
            encodedKey.Key.CopyTo(output.ToSpan());
            
            var outputSpan = output.ToSpan();
            key = outputSpan[^1] == 0 ? outputSpan[0..^1] : outputSpan;
            return true;
        }

        private static void GetEntryBuffer(Page page, ushort entryOffset, out byte* b, out int len)
        {
            byte* entryPos = b = page.Pointer + entryOffset;
            var keyLen = VariableSizeEncoding.ReadCompact<int>(entryPos, out var lenKeyLen, out bool success);
            if (success == false)
                InvalidBufferContent();

            ZigZagEncoding.DecodeCompact<long>(entryPos + keyLen + lenKeyLen, out var valLen, out success);
            if (success == false)
                InvalidBufferContent();

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

        private EncodedKey FuzzySearchPageAndPushNext(EncodedKey key, ref IteratorCursorState cstate)
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
