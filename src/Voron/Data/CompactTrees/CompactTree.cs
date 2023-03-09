using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Server;
using Sparrow.Server.Collections;
using Voron.Data.BTrees;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using static Voron.Data.CompactTrees.CompactTree;

namespace Voron.Data.CompactTrees
{
    public sealed unsafe partial class CompactTree
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


        // The reason why we use 2 is that the worst case scenario is caused by splitting pages where a single
        // page split will need 2 simultaneous key encoders. In this way we can get away with not instantiating
        // so many. 
        private CompactKey _internalKeyCache1;
        private CompactKey _internalKeyCache2;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal CompactKey AcquireKey()
        {
            if (_internalKeyCache1 == null && _internalKeyCache2 == null)
                return new CompactKey(this);

            CompactKey current;
            if (_internalKeyCache1 != null)
            {
                current = _internalKeyCache1;
                _internalKeyCache1 = null;
            }
            else
            {
                current = _internalKeyCache2;
                _internalKeyCache2 = null;
            }

            return current;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReleaseKey(CompactKey key)
        {
            if (_internalKeyCache1 != null && _internalKeyCache2 != null)
            {
                key.Dispose();
                return;
            }

            if (_internalKeyCache1 == null)
            {
                _internalKeyCache1 = key;
            }
            else
            {
                // We know we may be forcing the death of a key, but we are doing so because we are 
                // going to be reclaiming one of them anyways. 
                _internalKeyCache2 = key;
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
                var entriesOffsetsPtr = EntriesOffsetsPtr;
                for (int i = 0; i < Header->NumberOfEntries; i++)
                {
                    GetEntryBufferPtr(Page, entriesOffsetsPtr[i], out var len);
                    Debug.Assert(len < Constants.CompactTree.MaximumKeySize);
                    usedSpace += len;
                }

                int computedFreeSpace = (Constants.Storage.PageSize - usedSpace);
                Debug.Assert(computedFreeSpace >= 0);
                return computedFreeSpace;
            }
            public string DumpPageDebug(CompactTree tree)
            {
                var dictionary = CompactTree.GetEncodingDictionary(tree._llt, Header->DictionaryId);

                Span<byte> tempBuffer = stackalloc byte[2048];

                var sb = new StringBuilder();
                int total = 0;
                for (int i = 0; i < Header->NumberOfEntries; i++)
                {
                    total += tree.GetEncodedEntry(Page, EntriesOffsets[i], out var key, out int keyLengthInBits, out var l);

                    var decodedKey = tempBuffer;
                    dictionary.Decode(keyLengthInBits, key, ref decodedKey);

                    sb.AppendLine($" - {Encoding.UTF8.GetString(decodedKey)} - {l}");
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

        private readonly Tree _parent;

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

            CompactTreeDumper.WriteCommit(this);
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
            FindPageFor(key, ref _internalCursor);

            return ReturnValue(ref _internalCursor._stk[_internalCursor._pos], out value);
        }

        public bool TryGetValue(CompactKey key, out long value)
        {
            FindPageFor(key, ref _internalCursor);
            return ReturnValue(ref _internalCursor._stk[_internalCursor._pos], out value);
        }

        private static bool ReturnValue(ref CursorState state, out long value)
        {
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

        public bool TryGetNextValue(CompactKey key, out long value)
        {
            ref var state = ref _internalCursor._stk[_internalCursor._pos];
            if (state.Header->PageFlags == CompactPageFlags.Branch)
            {
                // the *previous* search didn't find a value, we are on a branch page that may
                // be correct or not, try first to search *down*
                key.ChangeDictionary(state.Header->DictionaryId);

                FindPageFor(ref _internalCursor, ref state, key);
                state = ref _internalCursor._stk[_internalCursor._pos];

                if (state.LastMatch == 0) // found it
                    return ReturnValue(ref state, out value);
                // did *not* find it, but we are somewhere on the tree that is ensured
                // to be at the key location *or before it*, so we can now start scanning *up*
            }
            Debug.Assert(state.Header->PageFlags == CompactPageFlags.Leaf, $"Got {state.Header->PageFlags} flag instead of {nameof(CompactPageFlags.Leaf)}");

            key.ChangeDictionary(state.Header->DictionaryId);

            SearchInCurrentPage(key, ref state);
            if (state.LastSearchPosition >= 0) // found it, yeah!
            {
                value = GetValue(ref state, state.LastSearchPosition);
                return true;
            }

            var pos = ~state.LastSearchPosition;
            var shouldBeInCurrentPage = pos < state.Header->NumberOfEntries;
            if (shouldBeInCurrentPage)
            {
                var nextEntryPtr = GetEncodedKeyPtr(state.Page, state.EntriesOffsetsPtr[pos], out var nextEntryLengthInBits);
                var match = key.CompareEncodedWith(nextEntryPtr, nextEntryLengthInBits, state.Header->DictionaryId);

                shouldBeInCurrentPage = match < 0;
            }

            if (shouldBeInCurrentPage == false)
            {
                // if this isn't in this page, it may be in the _next_ one, but we 
                // now need to check the parent page to see that
                shouldBeInCurrentPage = true;

                // TODO: Figure out if we can get rid of this copy and just change the current and restore after the loop. 
                using var currentKeyScope = new CompactKeyCacheScope(this, key);

                var currentKeyInPageDictionary = currentKeyScope.Key;
                
                for (int i = _internalCursor._pos - 1; i >= 0; i--)
                {
                    ref var cur = ref _internalCursor._stk[i];
                    if (cur.LastSearchPosition + 1 >= cur.Header->NumberOfEntries)
                        continue;

                    // We change the current dictionary for this key. 
                    var currentKeyInPageDictionaryPtr = GetEncodedKeyPtr(cur.Page, cur.EntriesOffsetsPtr[cur.LastSearchPosition + 1], out var currentKeyInPageDictionaryLengthInBits);

                    // PERF: The reason why we are changing the dictionary instead of comparing with a dictionary instead is because we want
                    // to explicitly exploit the fact that when dictionaries do not change along the search path, we can use the fast-path
                    // to find the encoded key. 
                    long dictionaryId = cur.Header->DictionaryId;
                    currentKeyInPageDictionary.ChangeDictionary(dictionaryId);
                    var match = currentKeyInPageDictionary.CompareEncodedWith(currentKeyInPageDictionaryPtr, currentKeyInPageDictionaryLengthInBits, dictionaryId);
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

                key.ChangeDictionary(state.Header->DictionaryId);
                SearchInCurrentPage(key, ref state);

                if (state.LastSearchPosition < 0)
                    state.LastSearchPosition = ~state.LastSearchPosition;

                // is this points to a different page, just search there normally
                if (state.LastSearchPosition > previousSearchPosition && state.LastSearchPosition < state.Header->NumberOfEntries)
                {
                    FindPageFor(ref _internalCursor, ref state, key);
                    return ReturnValue(ref _internalCursor._stk[_internalCursor._pos], out value);
                }
            }

            // if we go to here, we are at the root, so operate normally
            return TryGetValue(key, out value);

        }

        public bool TryGetNextValue(ReadOnlySpan<byte> key, out long value, out CompactKeyCacheScope cacheScope)
        {
            cacheScope = new CompactKeyCacheScope(this, key);
            var encodedKey = cacheScope.Key;

            ref var state = ref _internalCursor._stk[_internalCursor._pos];
            if (state.Header->PageFlags == CompactPageFlags.Branch)
            {
                // the *previous* search didn't find a value, we are on a branch page that may
                // be correct or not, try first to search *down*
                encodedKey.ChangeDictionary(state.Header->DictionaryId);

                FindPageFor(ref _internalCursor, ref state, encodedKey);
                state = ref _internalCursor._stk[_internalCursor._pos];

                if (state.LastMatch == 0) // found it
                    return ReturnValue(ref state, out value);
                // did *not* find it, but we are somewhere on the tree that is ensured
                // to be at the key location *or before it*, so we can now start scanning *up*
            }
            Debug.Assert(state.Header->PageFlags == CompactPageFlags.Leaf, $"Got {state.Header->PageFlags} flag instead of {nameof(CompactPageFlags.Leaf)}");

            encodedKey.ChangeDictionary(state.Header->DictionaryId);

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
                var nextEntryPtr = GetEncodedKeyPtr(state.Page, state.EntriesOffsetsPtr[pos], out var nextEntryLengthInBits);

                var match = encodedKey.CompareEncodedWith(nextEntryPtr, nextEntryLengthInBits, state.Header->DictionaryId);

                shouldBeInCurrentPage = match < 0;
            }

            if (shouldBeInCurrentPage == false)
            {
                // if this isn't in this page, it may be in the _next_ one, but we 
                // now need to check the parent page to see that
                shouldBeInCurrentPage = true;

                // TODO: Figure out if we can get rid of this copy and just change the current and restore after the loop. 
                using var currentKeyScope = new CompactKeyCacheScope(this, encodedKey);

                var currentKeyInPageDictionary = currentKeyScope.Key;
                for (int i = _internalCursor._pos - 1; i >= 0; i--)
                {
                    ref var cur = ref _internalCursor._stk[i];
                    if (cur.LastSearchPosition + 1 >= cur.Header->NumberOfEntries)
                        continue;

                    // We change the current dictionary for this key. 
                    var currentKeyInPageDictionaryPtr = GetEncodedKeyPtr(cur.Page, cur.EntriesOffsetsPtr[cur.LastSearchPosition + 1], out var currentKeyInPageDictionaryLengthInBits);

                    // PERF: The reason why we are changing the dictionary instead of comparing with a dictionary instead is because we want
                    // to explicitly exploit the fact that when dictionaries do not change along the search path, we can use the fast-path
                    // to find the encoded key. 
                    long dictionaryId = cur.Header->DictionaryId;
                    currentKeyInPageDictionary.ChangeDictionary(dictionaryId);
                    var match = currentKeyInPageDictionary.CompareEncodedWith(currentKeyInPageDictionaryPtr, currentKeyInPageDictionaryLengthInBits, dictionaryId);
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

                encodedKey.ChangeDictionary(state.Header->DictionaryId);
                SearchInCurrentPage(encodedKey, ref state);

                if (state.LastSearchPosition < 0)
                    state.LastSearchPosition = ~state.LastSearchPosition;
        
                // is this points to a different page, just search there normally
                if (state.LastSearchPosition > previousSearchPosition && state.LastSearchPosition < state.Header->NumberOfEntries )
                {
                    FindPageFor(ref _internalCursor, ref state, encodedKey);
                    return ReturnValue(ref _internalCursor._stk[_internalCursor._pos], out value);
                }
            }
            
            // if we go to here, we are at the root, so operate normally
            return TryGetValue(encodedKey, out value);
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
            using var scope = new CompactKeyCacheScope(this);
            var encodedKey = scope.Key;
            encodedKey.Set(key);
            FindPageFor(encodedKey, ref _internalCursor, tryRecompress: true);

            return RemoveFromPage(allowRecurse: true, out oldValue);
        }

        public bool TryRemove(CompactKey key, out long oldValue)
        {
            CompactTreeDumper.WriteRemoval(this, key.Decoded());

            FindPageFor(key, ref _internalCursor, tryRecompress: true);
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

            var keyLengthInBits = *(ushort*)entry;
            int keyLength = Bits.ToBytes(keyLengthInBits);

            int entryPosIdx = sizeof(ushort) + keyLength;
            oldValue = ZigZagEncoding.Decode<long>(entry + entryPosIdx, out var valLen);

            var totalEntrySize = entryPosIdx + valLen;
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

            var srcDictionary = GetEncodingDictionary(this._llt, sourceHeader->DictionaryId);
            var destDictionary = GetEncodingDictionary(this._llt, destinationHeader->DictionaryId);
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
            using var scope = new CompactKeyCacheScope(this);
            var newKey = scope.Key;

            var encodedKey = GetEncodedKey(sourcePage, sourceState.EntriesOffsets[0], out int encodedKeyLengthInBits);
            newKey.Set(encodedKeyLengthInBits, encodedKey, sourceHeader->DictionaryId);

            PopPage(ref _internalCursor);
            
            // we aren't _really_ removing, so preventing merging of parents
            RemoveFromPage(allowRecurse: false, parent.LastSearchPosition + 1);

            // Ensure that we got the right key to search. 
            newKey.ChangeDictionary(_internalCursor._stk[_internalCursor._pos].Header->DictionaryId);

            SearchInCurrentPage(newKey, ref _internalCursor._stk[_internalCursor._pos]); // positions changed, re-search
            AddToPage(newKey, siblingPage);
            return true;

            [SkipLocalsInit]
            bool MoveEntryWithReEncoding(Span<byte> decodeBuffer, Span<byte> encodeBuffer, ref CursorState destinationState, Span<ushort> entries)
            {
                // PERF: This method is marked SkipLocalInit because we want to avoid initialize these values
                // as we are going to be writing them anyways.
                byte* valueEncodingBuffer = stackalloc byte[16];

                // We get the encoded key and value from the sibling page
                var sourceEntrySize = GetEncodedEntry(sourcePage, sourceState.EntriesOffsetsPtr[sourceKeysCopied], out var encodedKey, out int encodedKeyLengthInBits, out var val);
                
                // If they have a different dictionary, we need to re-encode the entry with the new dictionary.
                if (encodedKey.Length != 0)
                {
                    var decodedKey = decodeBuffer;
                    srcDictionary.Decode(encodedKeyLengthInBits, encodedKey, ref decodedKey);

                    encodedKey = encodeBuffer;
                    destDictionary.Encode(decodedKey, ref encodedKey, out encodedKeyLengthInBits);
                }

                // We encode the length of the key and the value with variable length in order to store them later. 
                int valueLength = ZigZagEncoding.Encode(valueEncodingBuffer, val);

                // If we don't have enough free space in the receiving page, we move on. 
                var requiredSize = encodedKey.Length + sizeof(ushort) + valueLength;
                if (requiredSize + sizeof(ushort) > destinationState.Header->Upper - destinationState.Header->Lower)
                    return false; // done moving entries

                sourceMovedLength += sourceEntrySize;

                // We will update the entries offsets in the receiving page.
                destinationHeader->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
                destinationHeader->Upper -= (ushort)requiredSize;
                destinationHeader->Lower += sizeof(ushort);
                entries[sourceKeysCopied] = destinationHeader->Upper;

                // We are going to be storing in the following format:
                // [ keySizeInBits: ushort | key: sequence<byte> | value: varint ]
                var entryPos = destinationPage.Pointer + destinationHeader->Upper;
                *(ushort*)entryPos = (ushort)encodedKeyLengthInBits;
                entryPos += sizeof(ushort);
                encodedKey.CopyTo(new Span<byte>(entryPos, (int)(destinationPage.Pointer + Constants.Storage.PageSize - entryPos)));
                entryPos += encodedKey.Length;
                Memory.Copy(entryPos, valueEncodingBuffer, valueLength);                                                                

                Debug.Assert(destinationHeader->Upper >= destinationHeader->Lower);

                return true;
            }
      
            bool MoveEntryAsIs(ref CursorState destinationState, Span<ushort> entries)
            {
                // We get the encoded key and value from the sibling page
                var entry = GetEncodedEntryBuffer(sourcePage, sourceState.EntriesOffsetsPtr[sourceKeysCopied]);
                
                // If we don't have enough free space in the receiving page, we move on. 
                var requiredSize = entry.Length;
                if (requiredSize + sizeof(ushort) > destinationState.Header->Upper - destinationState.Header->Lower)
                    return false; // done moving entries

                sourceMovedLength += entry.Length;
                // We will update the entries offsets in the receiving page.
                destinationHeader->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
                destinationHeader->Upper -= (ushort)requiredSize;
                destinationHeader->Lower += sizeof(ushort);
                entries[sourceKeysCopied] = destinationHeader->Upper;

                // We copy the actual entry [ keySizeInBits: ushort | key: sequence<byte> | value: varint ] to the receiving page.
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

        private void AssertValueAndKeySize(CompactKey key, long value)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "Only positive values are allowed");
            if (key.MaxLength > Constants.CompactTree.MaximumKeySize)
                throw new ArgumentOutOfRangeException(nameof(key), Encoding.UTF8.GetString(key.Decoded()), $"key must be less than {Constants.CompactTree.MaximumKeySize} bytes in size");
        }

        private void AssertValueAndKeySize(ReadOnlySpan<byte> key, long value)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "Only positive values are allowed");
            if (key.Length > Constants.CompactTree.MaximumKeySize)
                throw new ArgumentOutOfRangeException(nameof(key), Encoding.UTF8.GetString(key), $"key must be less than {Constants.CompactTree.MaximumKeySize} bytes in size");
            if (key.Length <= 0)
                throw new ArgumentOutOfRangeException(nameof(key),  "key must be at least 1 byte");
        }

        public void Add(string key, long value)
        {
            using var _ = Slice.From(_llt.Allocator, key, out var slice);
            using var scope = new CompactKeyCacheScope(this, slice.AsReadOnlySpan());
            Add(scope.Key, value);
        }

        public void Add(ReadOnlySpan<byte> key, long value)
        {
            using var scope = new CompactKeyCacheScope(this, key);
            Add(scope.Key, value);
        }

        public void Add(CompactKey key, long value)
        {
            CompactTreeDumper.WriteAddition(this, key.Decoded(), value);

            AssertValueAndKeySize(key, value);

            FindPageFor(key, ref _internalCursor, tryRecompress: true);
            
            // this overload assumes that a previous call to TryGetValue (where you go the encodedKey
            // already placed us in the right place for the value)
            Debug.Assert(_internalCursor._stk[_internalCursor._pos].Header->PageFlags == CompactPageFlags.Leaf,
                $"Got {_internalCursor._stk[_internalCursor._pos].Header->PageFlags} flag instead of {nameof(CompactPageFlags.Leaf)}");

            AddToPage(key, value);
        }

        [SkipLocalsInit]
        private void AddToPage(CompactKey key, long value)
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
                var valuePtr = GetValuePtr(ref state, state.LastSearchPosition);
                ZigZagEncoding.Decode<long>(valuePtr, out var len);

                // TODO: Check if the valueBufferLength is bigger, shouldn't we be able to just update if the encoding is actually smaller?
                if (len == valueBufferLength)
                {
                    Debug.Assert(valueBufferLength <= sizeof(long));
                    Unsafe.CopyBlockUnaligned(valuePtr, valueBufferPtr, (uint)valueBufferLength);
                    return;
                }

                // remove the entry, we'll need to add it as new
                int entriesCount = state.Header->NumberOfEntries;
                ushort* stateEntriesOffsetsPtr = state.EntriesOffsetsPtr;
                GetEntryBufferPtr(state.Page, state.EntriesOffsets[state.LastSearchPosition], out var totalEntrySize);
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

            // We are going to be storing in the following format:
            // [ keySizeInBits: ushort | key: sequence<byte> | value: varint ]
            var encodedKey = key.EncodedWithCurrent(out int encodedKeyLengthInBits);

            var requiredSize = sizeof(ushort) + encodedKey.Length + valueBufferLength;
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
                    if (TryRecompressPage(ref state))   
                    {                        
                        Debug.Assert(state.Header->FreeSpace >= (state.Header->Upper - state.Header->Lower));
  
                        // Since the recompressing has changed the topology of the entire page, we need to reencode the key
                        // to move forward. 
                        key.ChangeDictionary(state.Header->DictionaryId);

                        // We need to recompute this because it will change.
                        encodedKey = key.EncodedWithCurrent(out encodedKeyLengthInBits);
                        requiredSize = sizeof(ushort) + encodedKey.Length + valueBufferLength;

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
                    //DebugStuff.RenderAndShow(this);
                    SplitPage(key, value);
                    //DebugStuff.RenderAndShow(this);
                    return;
                }
            }

            AddEntryToPage(key, state, requiredSize, valueBufferPtr, valueBufferLength);
            VerifySizeOf(ref state);
        }

        private void AddEntryToPage(CompactKey key, CursorState state, int requiredSize, byte* valueBufferPtr, int valueBufferLength)
        {
            //VerifySizeOf(ref state);

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

            // We are going to be storing in the following format:
            // [ keySizeInBits: ushort | key: sequence<byte> | value: varint ]
            byte* writePos = state.Page.Pointer + state.Header->Upper;
    
            var encodedKey = key.EncodedWithCurrent(out var encodedKeyInBits);
            *(ushort*)writePos = (ushort) encodedKeyInBits;
            writePos += sizeof(ushort);

            encodedKey.CopyTo(new Span<byte>(writePos, encodedKey.Length));
            writePos += encodedKey.Length;
            Unsafe.CopyBlockUnaligned(writePos, valueBufferPtr, (uint)valueBufferLength);
            newEntriesOffsets[state.LastSearchPosition] = state.Header->Upper;
            //VerifySizeOf(ref state);
        }

        private CompactKey SplitPage(CompactKey currentCauseForSplit, long value)
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

            splitKey.ChangeDictionary(_internalCursor._stk[_internalCursor._pos].Header->DictionaryId);

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

        private CompactKey SplitPageEncodedEntries(CompactKey causeForSplit, Page page, CompactPageHeader* header, long value, ref CursorState state)
        {
            var valueBufferPtr = stackalloc byte[10];
            int valueBufferLength = ZigZagEncoding.Encode(valueBufferPtr, value);

            var encodedKey = causeForSplit.EncodedWithCurrent(out var encodedKeyLengthInBits);

            var requiredSize = encodedKey.Length + sizeof(ushort) + valueBufferLength;
          
            var newPageState = new CursorState { Page = page };

            // sequential write up, no need to actually split
            int numberOfEntries = state.Header->NumberOfEntries;
            if (numberOfEntries == state.LastSearchPosition && state.LastMatch > 0)
            {
                newPageState.LastSearchPosition = 0; // add as first
                AddEntryToPage(causeForSplit, newPageState, requiredSize, valueBufferPtr, valueBufferLength);
                return causeForSplit;
            }

            // non sequential write, let's just split in middle
            int entriesCopied = 0;
            int sizeCopied = 0;
            ushort* offsets = newPageState.EntriesOffsetsPtr;
            int i = FindPositionToSplitPageInHalfBasedOfEntriesSize(ref state);

            for (; i < numberOfEntries; i++)
            {
                var entry = GetEntryBufferPtr(state.Page, state.EntriesOffsets[i], out var entryLength);

                header->Lower += sizeof(ushort);
                header->Upper -= (ushort)entryLength;
                header->FreeSpace -= (ushort)(entryLength + sizeof(ushort));
                sizeCopied += entryLength + sizeof(ushort);
                offsets[entriesCopied++] = header->Upper;
                Memory.Copy(page.Pointer + header->Upper, entry, entryLength);
            }
            state.Header->Lower -= (ushort)(sizeof(ushort) * entriesCopied);
            state.Header->FreeSpace += (ushort)(sizeCopied);

            DefragPage(_llt, ref state); // need to ensure that we have enough space to add the new entry in the source page
            
            var lastEntryFromPreviousPage = GetEncodedKey(state.Page, state.EntriesOffsets[state.Header->NumberOfEntries - 1], out var _);
            ref CursorState updatedPageState = ref newPageState; // start with the new page
            if (lastEntryFromPreviousPage.SequenceCompareTo(encodedKey) >= 0)
            {
                // the new entry belong on the *old* page
                updatedPageState = ref state;
            }
            
            SearchInCurrentPage(causeForSplit, ref updatedPageState);
            Debug.Assert(updatedPageState.LastSearchPosition < 0, "There should be no updates here");
            updatedPageState.LastSearchPosition = ~updatedPageState.LastSearchPosition;
            Debug.Assert(updatedPageState.Header->Upper - updatedPageState.Header->Lower >= requiredSize);
            AddEntryToPage(causeForSplit, updatedPageState, requiredSize, valueBufferPtr, valueBufferLength);

            VerifySizeOf(ref newPageState);
            VerifySizeOf(ref state);
            
            var pageEntries = new Span<ushort>(page.Pointer + PageHeader.SizeOf, header->NumberOfEntries);
            GetEncodedEntry(page, pageEntries[0], out var splitKey, out var splitKeyLengthInBits, out _);

            causeForSplit.Set(splitKeyLengthInBits, splitKey, ((CompactPageHeader*)page.Pointer)->DictionaryId);
            return causeForSplit;
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
                GetEntryBufferPtr(state.Page, state.EntriesOffsets[i], out var len);
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

#if !DEBUG
        [SkipLocalsInit]
#endif
        private bool TryRecompressPage(ref CursorState state)
        {
            // The finding process may call Recompress to avoid the expensive re-encoding and decoding of keys,
            // and instead upgrade the page. However, since TryRecompressPage may also be called from the finding
            // process, the page may not yet be marked as 'writable'.
            state.Page = _llt.ModifyPage(state.Page.PageNumber);

            var oldDictionary = CompactTree.GetEncodingDictionary(this._llt, state.Header->DictionaryId);
            var newDictionary = CompactTree.GetEncodingDictionary(this._llt, _state.TreeDictionaryId);

            using var _ = _llt.Environment.GetTemporaryPage(_llt, out var tmp);
            Memory.Copy(tmp.TempPagePointer, state.Page.Pointer, Constants.Storage.PageSize);

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

            for (int i = 0; i < tmpHeader->NumberOfEntries; i++)
            {
                GetEncodedEntry(tmpPage, oldEntries[i], out var encodedKey, out var encodedKeyLengthInBits, out var val);

                if (encodedKey.Length != 0)
                {
                    var decodedKey = decodeBuffer;
                    oldDictionary.Decode(encodedKeyLengthInBits, encodedKey, ref decodedKey);

                    encodedKey = encodeBuffer;
                    newDictionary.Encode(decodedKey, ref encodedKey, out encodedKeyLengthInBits);
                }

                int valueLength = ZigZagEncoding.Encode(valueBufferPtr, val);

                // It may very well happen that there is no enough encoding space to upgrade the page
                // because of an slightly inefficiency at this particular page. In those cases, we wont
                // upgrade the page and just fail. 
                var requiredSize = encodedKey.Length + sizeof(ushort) + valueLength;
                if (requiredSize + sizeof(ushort) > state.Header->FreeSpace)
                    goto Failure;

                state.Header->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
                state.Header->Lower += sizeof(ushort);
                state.Header->Upper -= (ushort)requiredSize;
                newEntries[i] = state.Header->Upper;

                var entryPos = state.Page.Pointer + state.Header->Upper;
                *(ushort*)entryPos = (ushort)encodedKeyLengthInBits;
                entryPos += sizeof(ushort);
                encodedKey.CopyTo(new Span<byte>(entryPos, (int)(state.Page.Pointer + Constants.Storage.PageSize - entryPos)));
                entryPos += encodedKey.Length;
                Memory.Copy(entryPos, valueBufferPtr, valueLength);
            }

            Debug.Assert(state.Header->FreeSpace == (state.Header->Upper - state.Header->Lower));

            state.Header->DictionaryId = newDictionary.PageNumber;
            _llt._persistentDictionariesForCompactTrees.Add(newDictionary.PageNumber, newDictionary);

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
            var size = sizeof(ushort) + pageNumberLength;

            state.Header->Upper = (ushort)(Constants.Storage.PageSize - size);
            state.Header->FreeSpace -= (ushort)(size + sizeof(ushort));

            state.EntriesOffsetsPtr[0] = state.Header->Upper;
            byte* entryPos = state.Page.Pointer + state.Header->Upper;

            // This is a zero length key (which is the start of the tree).
            *(ushort*)entryPos = 0; // zero len key
            entryPos += sizeof(ushort);
            Memory.Copy(entryPos, pageNumberBufferPtr, pageNumberLength);

            Debug.Assert(state.Header->FreeSpace == (state.Header->Upper - state.Header->Lower));

            InsertToStack(state with { Page = page });
            state.LastMatch = -1;
            state.LastSearchPosition = 0;
        }

        private void InsertToStack(in CursorState newPageState)
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
                    var entryBuffer = GetEntryBufferPtr(state.Page, sourceEntriesOffsets[i], out var len);
                    Debug.Assert((tmpHeader->Upper - len) > 0);

                    ushort lowerIndex = (ushort)(tmpHeader->Upper - len);

                    // Note: Since we are just defragmenting, FreeSpace doesn't change.
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

            using var scope = new CompactKeyCacheScope(this);
            for (ushort i = 0; i < state.Header->NumberOfEntries; i++)
            {
                GetEncodedEntry(page, state.EntriesOffsets[i], out var encodedKey, out var encodedKeyLengthInBits, out var val);
                scope.Key.Set(encodedKeyLengthInBits, encodedKey, state.Header->DictionaryId);

                results.Add((Encoding.UTF8.GetString(scope.Key.Decoded()), val));
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void FindPageFor(ReadOnlySpan<byte> key, ref IteratorCursorState cstate)
        {
            using var scope = new CompactKeyCacheScope(this);
            var encodedKey = scope.Key;
            encodedKey.Set(key);
            FindPageFor(encodedKey, ref cstate);
        }

        private void FindPageFor(CompactKey key, ref IteratorCursorState cstate, bool tryRecompress = false)
        {
            cstate._pos = -1;
            cstate._len = 0;
            PushPage(_state.RootPage, ref cstate);

            ref var state = ref cstate._stk[cstate._pos];

            key.ChangeDictionary(state.Header->DictionaryId);

            FindPageFor(ref cstate, ref state, key, tryRecompress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void FindPageFor(ref IteratorCursorState cstate, ref CursorState state, CompactKey compactKey, bool tryRecompress = false)
        {
            while (state.Header->PageFlags.HasFlag(CompactPageFlags.Branch))
            {
                // PERF: We aim to carry out the dictionary migration during the search process. This is because even though it may take longer,
                // we can offset the cost of future key re-encoding along the way. We could choose to do this either before or after
                // adding/searching, but performing it proactively guarantees that we avoid the need for re-encoding at this stage.
                if (tryRecompress && _state.TreeDictionaryId != state.Header->DictionaryId)
                    TryRecompressPage(ref state);

                compactKey.ChangeDictionary(cstate._stk[cstate._pos].Header->DictionaryId);
                SearchInCurrentPage(compactKey, ref cstate._stk[cstate._pos]);

                ref var nState = ref cstate._stk[cstate._pos];
                if (nState.LastSearchPosition < 0)
                    nState.LastSearchPosition = ~nState.LastSearchPosition;
                if (nState.LastMatch != 0 && nState.LastSearchPosition > 0)
                    nState.LastSearchPosition--; // went too far

                int actualPos = Math.Min(nState.Header->NumberOfEntries - 1, nState.LastSearchPosition);
                var nextPage = GetValue(ref nState, actualPos);

                PushPage(nextPage, ref cstate);

                state = ref cstate._stk[cstate._pos];
            }

            if (tryRecompress && _state.TreeDictionaryId != state.Header->DictionaryId)
                TryRecompressPage(ref state);

            compactKey.ChangeDictionary(cstate._stk[cstate._pos].Header->DictionaryId);
            SearchInCurrentPage(compactKey, ref state);
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
            if (cstate._pos > cstate._stk.Length) //  should never actually happen
                Array.Resize(ref cstate._stk, cstate._stk.Length * 2); // but let's be safe

            ref var state = ref cstate._stk[++cstate._pos];
            state.Page = _llt.GetPage(nextPage);

            cstate._len++;
        }

        internal static PersistentDictionary GetEncodingDictionary(LowLevelTransaction llt, long dictionaryId)
        {
            llt._persistentDictionariesForCompactTrees ??= new WeakSmallSet<long, PersistentDictionary>(16);
            if (llt._persistentDictionariesForCompactTrees.TryGetValue(dictionaryId, out var dictionary))
                return dictionary;

            dictionary = new PersistentDictionary(llt.GetPage(dictionaryId));
            llt._persistentDictionariesForCompactTrees.Add(dictionaryId, dictionary);
            return dictionary;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte* GetEncodedKeyPtr(Page page, ushort entryOffset, out int lengthInBits)
        {
            var entryPos = (page.Pointer + entryOffset);
            lengthInBits = *(ushort*)entryPos;
            return entryPos + sizeof(ushort);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ReadOnlySpan<byte> GetEncodedKey(Page page, ushort entryOffset, out int lengthInBits)
        {
            var entryPos = (page.Pointer + entryOffset);
            lengthInBits = *(ushort*)entryPos;
            return new ReadOnlySpan<byte>(entryPos + sizeof(ushort), Bits.ToBytes(lengthInBits));
        }

        private static long GetValue(ref CursorState state, int pos)
        {
            var valuePtr = GetValuePtr(ref state, pos);
            var value = ZigZagEncoding.DecodeCompact<long>(valuePtr, out var valLen, out var success);
            if (success == false)
                InvalidBufferContent();

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte* GetValuePtr(ref CursorState state, int pos)
        {
            byte* entryPos = (state.Page.Pointer + state.EntriesOffsetsPtr[pos]);
            int keyLengthInBits = *(ushort*)entryPos;
            return entryPos + sizeof(ushort) + Bits.ToBytes(keyLengthInBits);
        }

        internal int GetEncodedEntry(Page page, ushort entryOffset, out Span<byte> key, out int keyLengthInBits, out long value)
        {
            if(entryOffset < PageHeader.SizeOf)
                throw new ArgumentOutOfRangeException();

            byte* entryPtr = (page.Pointer + entryOffset);
            keyLengthInBits = *(ushort*)entryPtr;

            int keyLen = Bits.ToBytes(keyLengthInBits);
            key = new Span<byte>(entryPtr + sizeof(ushort), keyLen);

            int valuePos = keyLen + sizeof(ushort);
            value = ZigZagEncoding.DecodeCompact<long>(entryPtr + valuePos, out var valLen, out var success);
            if (success == false)
                InvalidBufferContent();

            return valuePos + valLen;
        }

        private static void InvalidBufferContent()
        {
            throw new VoronErrorException("Invalid data found in the buffer.");
        }

        private static Span<byte> GetEncodedEntryBuffer(Page page, ushort entryOffset)
        {
            if(entryOffset < PageHeader.SizeOf)
                throw new ArgumentOutOfRangeException();

            byte* entry = (page.Pointer + entryOffset);
            int keyLengthInBits = *(ushort*)entry;
            byte* pos = entry + Bits.ToBytes(keyLengthInBits) + sizeof(ushort);

            ZigZagEncoding.DecodeCompact<long>(pos, out var lenOfValue, out var success);
            if (success == false)
                InvalidBufferContent();
            pos += lenOfValue;

            return new Span<byte>(entry, (int)(pos - entry));
        }

        internal static bool GetEntry(CompactTree tree, Page page, ushort entriesOffset, out CompactKeyCacheScope key, out long value)
        {
            tree.GetEncodedEntry(page, entriesOffset, out var encodedKeyStream, out var encodedKeyLengthInBits, out value);
            if (encodedKeyStream.Length == 0)
            {
                key = default;
                return false;
            }

            key = new CompactKeyCacheScope(tree);
            key.Key.Set(encodedKeyLengthInBits, encodedKeyStream, ((CompactPageHeader*)page.Pointer)->DictionaryId);

            return true;
        }

        private static byte* GetEntryBufferPtr(Page page, ushort entryOffset, out int length)
        {
            // Keep the initial ptr for the entry.
            byte* entryPtr = page.Pointer + entryOffset;
            
            // Get the length of the key.
            int keyLengthInBits = *(ushort*)entryPtr;
            int keyLength = Bits.ToBytes(keyLengthInBits);

            // Advance the index until where the value is.
            int entryPos = sizeof(ushort) + keyLength;
            // TODO: We don't really need the actual value. Can we do an optimized value length calculator?
            ZigZagEncoding.DecodeCompact<long>(entryPtr + entryPos, out var valLen, out var success);
            if (success == false)
                InvalidBufferContent();

            length = entryPos + valLen;
            return entryPtr;
        }

        private void SearchInCurrentPage(CompactKey key, ref CursorState state)
        {
            // Ensure that the key has already been 'updated' this is internal and shouldn't check explicitly that.
            // It is the responsibility of the caller to ensure that is the case. 
            Debug.Assert(key.Dictionary == state.Header->DictionaryId);

            ushort* @base = state.EntriesOffsetsPtr;
            int length = state.Header->NumberOfEntries;
            if (length == 0)
            {
                state.LastMatch = -1;
                state.LastSearchPosition = -1;
                return;
            }

            byte* keyPtr = key.EncodedWithPtr(state.Header->DictionaryId, out int keyLengthInBits);
            int keyLength = Bits.ToBytes(keyLengthInBits);

            int match;
            int bot = 0;
            int top = length;

            byte* encodedKeyPtr;
            int encodedKeyLength;
            int encodedKeyLengthInBits;
            while (top > 1)
            {
                int mid = top / 2;

                encodedKeyPtr = GetEncodedKeyPtr(state.Page, @base[bot + mid], out encodedKeyLengthInBits);

                encodedKeyLength = Bits.ToBytes(encodedKeyLengthInBits);
                match = AdvMemory.CompareInline(keyPtr, encodedKeyPtr, Math.Min(keyLength, encodedKeyLength));
                match = match == 0 ? keyLengthInBits - encodedKeyLengthInBits : match;

                if (match >= 0)
                    bot += mid;

                top -= mid;
            }

            encodedKeyPtr = GetEncodedKeyPtr(state.Page, @base[bot], out encodedKeyLengthInBits);

            encodedKeyLength = Bits.ToBytes(encodedKeyLengthInBits);
            match = AdvMemory.CompareInline(keyPtr, encodedKeyPtr, Math.Min(keyLength, encodedKeyLength));
            match = match == 0 ? keyLengthInBits - encodedKeyLengthInBits : match;
            if (match == 0)
            {
                state.LastMatch = 0;
                state.LastSearchPosition = bot;
                return;
            }

            state.LastMatch = match > 0 ? 1 : -1;
            state.LastSearchPosition = ~(bot + (match > 0).ToInt32());
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
                if (s2[i] < s1[i])
                    return 1;
            }

            if (len1 < len2)
                return -1;

            return 1;
        }

        private void FuzzySearchInCurrentPage(in CompactKey key, ref CursorState state)
        {
            // Ensure that the key has already been 'updated' this is internal and shouldn't check explicitly that.
            // It is the responsibility of the caller to ensure that is the case. 
            Debug.Assert(key.Dictionary == state.Header->DictionaryId);

            var encodedKey = key.EncodedWithCurrent(out var _);

            int high = state.Header->NumberOfEntries - 1, low = 0;
            int match = -1;
            int mid = 0;
            while (low <= high)
            {
                mid = (high + low) / 2;
                
                var currentKey = GetEncodedKey(state.Page, state.EntriesOffsetsPtr[mid], out var _);

                match = DictionaryOrder(encodedKey, currentKey);

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

        private void FuzzySearchPageAndPushNext(CompactKey key, ref IteratorCursorState cstate)
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

            // TODO: Most searches may only require transcoding only, not setting new values as we are looking for the key anyways. 
            key.ChangeDictionary(cstate._stk[cstate._pos].Header->DictionaryId);
        }

        private void FuzzyFindPageFor(ReadOnlySpan<byte> key, ref IteratorCursorState cstate)
        {
            // Algorithm 2: Find Node

            cstate._pos = -1;
            cstate._len = 0;
            PushPage(_state.RootPage, ref cstate);

            ref var state = ref cstate._stk[cstate._pos];

            using var scope = new CompactKeyCacheScope(this);
            
            var encodedKey = scope.Key;
            encodedKey.Set(key);
            encodedKey.ChangeDictionary(state.Header->DictionaryId);

            while (state.Header->PageFlags.HasFlag(CompactPageFlags.Branch))
            {
                FuzzySearchPageAndPushNext(encodedKey, ref cstate);
                state = ref cstate._stk[cstate._pos];
            }
                        
            // if N is the leaf node then
            //    Return N
            state.LastMatch = 1;
            state.LastSearchPosition = 0;
        }
    }
}
