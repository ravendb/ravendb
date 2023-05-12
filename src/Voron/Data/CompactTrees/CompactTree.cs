using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
using System.Text;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Voron.Data.BTrees;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.CompactTrees
{
    /// <summary>
    /// The compact tree is effectively a map[long,long] internally
    /// We support map[string,long] using *external* storage of the keys, holding on
    /// only to the location of the external key storage inside the tree itself.
    ///
    /// The tree items are stored as [4 bits - key len | 4 bits - val len] [ key ] [ val ]
    /// Since keys & values are int64, they all fit in under 8 bytes, which fits in 4 bits. W
    /// We chose this format instead of variable size ints to reduce the number of branches.
    ///
    /// The container entries hold the actual terms for the tree, with the idea of making this as compact
    /// as possible. The structure of the container entry is:
    /// [2 bytes - length in bits of the key] [key bytes] [1 byte - reference count]
    /// 
    /// A term may appear on multiple pages at the same time (if it is used as the split key in the structure
    /// That means that we have may have multiple references to it. A term may *also* appear multiple times using
    /// different dictionaries. We treat those as separate keys, however. They'll compare equal and work correctly, of course. 
    /// </summary>
    public sealed unsafe partial class CompactTree : IPrepareForCommit
    {
        public const int EncodingBufferSize = sizeof(long) + sizeof(long) + 1;
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int LeadingZeroes(ulong n)
        {
            if (Lzcnt.X64.IsSupported)
            {
                return (int)Lzcnt.X64.LeadingZeroCount(n);
            }

            if (ArmBase.Arm64.IsSupported)
            {
                return ArmBase.Arm64.LeadingZeroCount(n);
            }
            // manual way
            return Bits.LeadingZeroes(n);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int EncodeEntry(long key, long val, byte* buffer)
        {
            var keyLen = 8 - LeadingZeroes((ulong)key) / 8;
            var valLen = 8 - LeadingZeroes((ulong)val) / 8;
            Debug.Assert(keyLen <= 8 && valLen <= 8);
            buffer[0] = (byte)(keyLen << 4 | valLen);
            Memory.Copy(buffer + 1, &key, keyLen);
            Memory.Copy(buffer + 1 + keyLen, &val, valLen);
            return 1 + keyLen + valLen;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int DecodeEntry(byte* buffer, out long key, out long val)
        {
            var keyLen = buffer[0] >> 4;
            var valLen = buffer[0] & 0xF;
            long k = 0, v = 0;
            Memory.Copy(&k, buffer+1, keyLen);
            Memory.Copy(&v, buffer+1 + keyLen, valLen);
            key = k;
            val = v;
            return 1 + keyLen + valLen;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long EncodeKeyToPage(ref CursorState state, long keyContainerId)
        {
            Debug.Assert(keyContainerId >= 0);
            // we'll try to reduce the size of the container ids by subtracting
            // from the initial key in the page. In most cases, it'll lead to a 
            // value that is smaller than the original, but it shouldn't generate
            // a value that is *larger* than the previous one once encoded
            var diff = (keyContainerId - state.Header->ContainerBasePage);
            // the diff may be *negative*, so we use zig/zag encoding to handle this
            return (diff << 1) ^ (diff >> 63);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long DecodeKeyFromPage(ref CursorState state, long keyContainerDiff)
        {
            if ((keyContainerDiff & 1) == 0)
                return state.Header->ContainerBasePage + (keyContainerDiff >>> 1);
            
            return state.Header->ContainerBasePage + ((keyContainerDiff >>> 1) ^ -1); 
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long DecodeKey(byte* buffer)
        {
            var keyLen = buffer[0] >> 4;
            long k = 0;
            Memory.Copy(&k, buffer+1, keyLen);
            return k;
        }
        
         
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long DecodeValue(byte* buffer)
        {
            var keyLen = buffer[0] >> 4;
            var valLen = buffer[0] & 0xF;
            long v = 0;
            Memory.Copy(&v, buffer+1 + keyLen, valLen);
            return v;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetEntryBuffer(ref CursorState state, int pos, out byte* buffer)
        {
            buffer = state.Page.Pointer + state.EntriesOffsetsPtr[pos];
            var keyLen = buffer[0] >> 4;
            var valLen = buffer[0] & 0xF;
            return keyLen + valLen + 1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetEntrySize(ref CursorState state, int pos) =>
            GetEntrySize(state.Page.Pointer + state.EntriesOffsetsPtr[pos]);
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetEntrySize(byte* header) => GetEntrySize(*header);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetEntrySize(byte header)
        {
            var keyLen = header >> 4;
            var valLen = header & 0xF;
            return 1 + keyLen + valLen;
        }
        
        private long WriteTermToContainer(ReadOnlySpan<byte> encodedKey, int encodedKeyLengthInBits, long dictionaryId)
        {
            // the term in the container is:  [ metadata -1 byte ] [ term bytes ]
            // the metadata is composed of two nibbles - the first says the *remainder* of bits in the last byte in the term
            // the second nibble is the reference count
            var id = Container.Allocate(_llt, _state.TermsContainerId, encodedKey.Length + 1, dictionaryId, out var allocated);
            encodedKey.CopyTo(allocated[1..]);
            var remainderBits = encodedKey.Length * 8 - encodedKeyLengthInBits;
            Debug.Assert(remainderBits is >= 0 and < 8);
            allocated[0] = (byte)(remainderBits << 4); // ref count of 0, will be incremented shortly
            return id;
        }
        
        
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private int CompareKeys(long currentKeyId, byte* keyPtr, int keyLength, int keyLengthInBits)
        {
            GetEncodedKeyPtr(currentKeyId, out byte* encodedKeyPtr, out var encodedKeyLength, out var encodedKeyLengthInBits);

            // CompactKey current = new(_llt);
            // current.Set(encodedKeyLengthInBits, new (encodedKeyPtr, encodedKeyLength), state.Header->DictionaryId);
            // var key = current.ToString();
            
            int match = AdvMemory.CompareInline(keyPtr, encodedKeyPtr, Math.Min(keyLength, encodedKeyLength));
            match = match == 0 ? keyLengthInBits - encodedKeyLengthInBits : match;
            return match;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private void GetEncodedKeyPtr(long currentKeyId, out byte* encodedKeyPtr, out int encodedKeyLen, out int encodedKeyLengthInBits)
        {
            if (currentKeyId == 0)
            {
                encodedKeyPtr = null;
                encodedKeyLengthInBits = 0;
                encodedKeyLen = 0;
                return;
            }

            Debug.Assert(currentKeyId > 0, "Negative container id is a bad sign");
            var keyItem = Container.Get(_llt, currentKeyId);
            int remainderInBits = *keyItem.Address >> 4;
            encodedKeyLen = keyItem.Length - 1;
            encodedKeyLengthInBits = encodedKeyLen * 8 - remainderInBits;
            encodedKeyPtr = keyItem.Address + 1;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private ReadOnlySpan<byte> GetEncodedKeySpan(ref CursorState state, int pos,  out int encodedKeyLengthInBits)
        {
            long currentKeyDiff = DecodeKey(state.Page.Pointer + state.EntriesOffsetsPtr[pos]);
            var currentKeyId = DecodeKeyFromPage(ref state, currentKeyDiff);
            GetEncodedKeyPtr(currentKeyId, out var encodedKeyPtr, out var encodedKeyLen, out encodedKeyLengthInBits);
            return new ReadOnlySpan<byte>(encodedKeyPtr, encodedKeyLen);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private Span<byte> GetEncodedKeySpan(ref CursorState state, int pos,  out int encodedKeyLengthInBits, out long currentKeyId, out long value)
        {
            DecodeEntry(state.Page.Pointer + state.EntriesOffsetsPtr[pos], out var currentKeyDiff, out value);
            currentKeyId = DecodeKeyFromPage(ref state, currentKeyDiff);
            GetEncodedKeyPtr(currentKeyId, out var encodedKeyPtr, out var encodedKeyLen, out encodedKeyLengthInBits);
            return new Span<byte>(encodedKeyPtr, encodedKeyLen);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private Span<byte> GetEncodedKeySpan(ref CursorState state, int pos, out int encodedKeyLengthInBits, out long value)
        {
            return GetEncodedKeySpan(ref state, pos,  out encodedKeyLengthInBits, out _, out value);
        }

        
        private void DecrementTermReferenceCount(long keyContainerId)
        {
            var term = Container.GetMutable(_llt, keyContainerId);
            int termRefCount = term[0] & 0xF;
            if(termRefCount == 0)
                throw new VoronErrorException("A term exists without any references? That should be impossible");
            if (termRefCount == 1) // no more references, can delete
            {
                Container.Delete(_llt, _state.TermsContainerId ,keyContainerId);
                return;
            }
            
            term[0] = (byte)( (term[0] & 0xF0) | (termRefCount - 1));
        }

        private void IncrementTermReferenceCount(long keyContainerId)
        {
            var term = Container.GetMutable(_llt, keyContainerId);
            int termRefCount = term[0] & 0xF;
            // A term usage count means that it is used by multiple pages at the same time. That can happen only if a leaf & branches are using
            // the same term as the separator. However, that has natural limits. As the term can only be in one path through the tree, the tree height
            // is a natural limit. Compact tree at maximum storage will take ~17 bytes, which means at *least* 425 items, which means that that the 
            // height of the tree if we store 2^64 items it in would be 8. So even if we assume bad insert factor, which will increase the height of the
            // tree, having a limit of 15 is reasonable
            if (termRefCount == 15)
                throw new VoronErrorException($"A term is used at max(tree-height), but we have term: {keyContainerId} used: {termRefCount}. This is a bug");

            term[0] = (byte)((term[0] & 0xF0) | termRefCount + 1);
        }


        
        private LowLevelTransaction _llt;
        internal CompactTreeState _state;
        
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

        public struct CursorState
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
                    usedSpace += GetEntrySize(Page.Pointer + entriesOffsetsPtr[i]);
                }

                int computedFreeSpace = (Constants.Storage.PageSize - usedSpace);
                Debug.Assert(computedFreeSpace >= 0);
                return computedFreeSpace;
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

                long containerId = Container.Create(llt);

                var newPage = llt.AllocatePage(1);
                var compactPageHeader = (CompactPageHeader*)newPage.Pointer;
                compactPageHeader->PageFlags = CompactPageFlags.Leaf;
                compactPageHeader->Lower = PageHeader.SizeOf;
                compactPageHeader->Upper = Constants.Storage.PageSize;
                compactPageHeader->FreeSpace = Constants.Storage.PageSize - (PageHeader.SizeOf);
                compactPageHeader->ContainerBasePage = ComputeContainerBasePage(llt, containerId);

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
                    TermsContainerId = containerId,
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

        private static long ComputeContainerBasePage(long containerKeyId)
        {
            return containerKeyId & 0x1FFF;
        }

        private static long ComputeContainerBasePage(LowLevelTransaction llt, long containerId)
        {
            return Container.GetNextFreePage(llt, containerId) * Constants.Storage.PageSize;
        }

        public void PrepareForCommit()
        {
            using var _ = _parent.DirectAdd(Name, sizeof(CompactTreeState), out var ptr);
            _state.CopyTo((CompactTreeState*)ptr);

            CompactTreeDumper.WriteCommit(this);
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
                } 
                while (state.Header->IsBranch);
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
            using var scope = new CompactKeyCacheScope(this._llt, key, State.TreeDictionaryId);

            return TryGetValue(scope.Key, out value);
        }
        
        public bool TryGetValue(CompactKey key, out long termContainerId, out long value)
        {
            key.ChangeDictionary(State.TreeDictionaryId);
            FindPageFor(key, ref _internalCursor);
            termContainerId = key.ContainerId;
            return ReturnValue(ref _internalCursor._stk[_internalCursor._pos], out value);
        }

        public bool TryGetValue(CompactKey key, out long value)
        {
            key.ChangeDictionary(State.TreeDictionaryId);
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

        public long DictionaryId => State.TreeDictionaryId;

        public void InitializeStateForTryGetNextValue()
        {
            _internalCursor._pos = -1;
            _internalCursor._len = 0;
            PushPage(_state.RootPage, ref _internalCursor);
            ref var state = ref _internalCursor._stk[_internalCursor._pos];
            state.LastSearchPosition = 0;
        }

        public bool TryGetNextValue(ReadOnlySpan<byte> key, out long termContainerId, out long value, out CompactKeyCacheScope cacheScope)
        {
            cacheScope = new CompactKeyCacheScope(this._llt, key, State.TreeDictionaryId);
            var encodedKey = cacheScope.Key;
            encodedKey.ChangeDictionary(_state.TreeDictionaryId);

            ref var state = ref _internalCursor._stk[_internalCursor._pos];
            if (state.Header->PageFlags == CompactPageFlags.Branch)
            {
                FindPageFor(ref _internalCursor, ref state, encodedKey);
                state = ref _internalCursor._stk[_internalCursor._pos];

                if (state.LastMatch == 0) // found it
                {
                    termContainerId = encodedKey.ContainerId;
                    return ReturnValue(ref state, out value);
                }
                // did *not* find it, but we are somewhere on the tree that is ensured
                // to be at the key location *or before it*, so we can now start scanning *up*
            }
            Debug.Assert(state.Header->PageFlags == CompactPageFlags.Leaf, $"Got {state.Header->PageFlags} flag instead of {nameof(CompactPageFlags.Leaf)}");

            SearchInCurrentPage(encodedKey, ref state);
            if (state.LastSearchPosition  >= 0) // found it, yeah!
            {
                Debug.Assert(encodedKey.ContainerId > 0);
                termContainerId = encodedKey.ContainerId;
                value = GetValue(ref state, state.LastSearchPosition);
                return true;
            }

            var pos = ~state.LastSearchPosition;
            var shouldBeInCurrentPage = pos < state.Header->NumberOfEntries;
            if (shouldBeInCurrentPage)
            {
                var match = CompareEntryWith(ref state, pos, encodedKey);
                    
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

                    var match = CompareEntryWith(ref cur, cur.LastSearchPosition + 1, encodedKey);
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
                termContainerId = -1;
                return false;
            }

            while (_internalCursor._pos > 0)
            {
                PopPage(ref _internalCursor);
                state = ref _internalCursor._stk[_internalCursor._pos];
                var previousSearchPosition = state.LastSearchPosition;

                SearchInCurrentPage(encodedKey, ref state);

                if (state.LastSearchPosition < 0)
                    state.LastSearchPosition = ~state.LastSearchPosition;
        
                // is this points to a different page, just search there normally
                if (state.LastSearchPosition > previousSearchPosition && state.LastSearchPosition < state.Header->NumberOfEntries )
                {
                    FindPageFor(ref _internalCursor, ref state, encodedKey);
                    termContainerId = encodedKey.ContainerId;
                    return ReturnValue(ref _internalCursor._stk[_internalCursor._pos], out value);
                }
            }
            
            // if we go to here, we are at the root, so operate normally
            return TryGetValue(encodedKey, out termContainerId, out value);
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
            using var scope = new CompactKeyCacheScope(this._llt, key, State.TreeDictionaryId);
            FindPageFor(scope.Key, ref _internalCursor);

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
            var len = DecodeEntry(entry, out var keyContainerDiff, out oldValue);
            var keyContainerId = DecodeKeyFromPage(ref state, keyContainerDiff);
            if (keyContainerId != 0)
            {
                DecrementTermReferenceCount(keyContainerId);
            }

            state.Header->FreeSpace += (ushort)(sizeof(ushort) + len);
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

            bool reEncode = sourceHeader->ContainerBasePage != destinationHeader->ContainerBasePage;

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
            
            var encodedKey = GetEncodedKeySpan(ref sourceState, 0, out int encodedKeyLengthInBits);

            PopPage(ref _internalCursor);
            
            // we aren't _really_ removing, so preventing merging of parents
            RemoveFromPage(allowRecurse: false, parent.LastSearchPosition + 1);

            // Ensure that we got the right key to search. 
            using var scope = new CompactKeyCacheScope(this._llt, encodedKeyLengthInBits, encodedKey, State.TreeDictionaryId);
            var newKey = scope.Key;

            SearchInCurrentPage(newKey, ref _internalCursor._stk[_internalCursor._pos]); // positions changed, re-search
            AddToPage(newKey, siblingPage);
            return true;

            [SkipLocalsInit]
            bool MoveEntryWithReEncoding(Span<byte> decodeBuffer, Span<byte> encodeBuffer, ref CursorState destinationState, Span<ushort> entries)
            {
                // PERF: This method is marked SkipLocalInit because we want to avoid initialize these values
                // as we are going to be writing them anyways.
                byte* entryBuffer = stackalloc byte[EncodingBufferSize];
                //We get the encoded key and value from the sibling page
                var entry = sourceState.Page.Pointer + sourceState.EntriesOffsetsPtr[sourceKeysCopied];
                var originalEntrySize = DecodeEntry(entry, out var keyContainerDiff, out var value);
                var keyContainerId = DecodeKeyFromPage(ref sourceState, keyContainerDiff);

                // If we don't have enough free space in the receiving page, we move on. 
                var requiredSize = EncodeEntry(EncodeKeyToPage(ref destinationState, keyContainerId), value, entryBuffer);
                if (requiredSize + sizeof(ushort) > destinationState.Header->Upper - destinationState.Header->Lower)
                {
                    return false; // done moving entries
                }
               
                sourceMovedLength += originalEntrySize;
                
                // We will update the entries offsets in the receiving page.
                destinationHeader->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
                destinationHeader->Upper -= (ushort)requiredSize;
                destinationHeader->Lower += sizeof(ushort);
                entries[sourceKeysCopied] = destinationHeader->Upper;
                
                // We are going to be storing in the following format:
                // [ keySizeInBits: ushort | key: sequence<byte> | value: varint ]
                var entryPos = destinationPage.Pointer + destinationHeader->Upper;
                Memory.Copy(entryPos, entryBuffer, requiredSize);                                                                
                
                Debug.Assert(destinationHeader->Upper >= destinationHeader->Lower);

                return true;
            }
      
            bool MoveEntryAsIs(ref CursorState destinationState, Span<ushort> entries)
            {
                // We get the encoded key and value from the sibling page
                var len = GetEntryBuffer(ref sourceState, sourceKeysCopied, out var buffer);
                
                // If we don't have enough free space in the receiving page, we move on. 
                if (len + sizeof(ushort) > destinationState.Header->Upper - destinationState.Header->Lower)
                    return false; // done moving entries
                
                sourceMovedLength += len;
                // We will update the entries offsets in the receiving page.
                destinationHeader->FreeSpace -= (ushort)(len + sizeof(ushort));
                destinationHeader->Upper -= (ushort)len;
                destinationHeader->Lower += sizeof(ushort);
                entries[sourceKeysCopied] = destinationHeader->Upper;

                Memory.Copy(destinationPage.Pointer + destinationHeader->Upper, buffer, len);
                
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

        public void Add(string key, long value)
        {
            using var _ = Slice.From(_llt.Allocator, key, out var slice);
            using var scope = new CompactKeyCacheScope(this._llt, slice.AsReadOnlySpan(), State.TreeDictionaryId);
            Add(scope.Key, value);
        }

        public void Add(ReadOnlySpan<byte> key, long value)
        {
            using var scope = new CompactKeyCacheScope(this._llt, key, State.TreeDictionaryId);
            Add(scope.Key, value);
        }

        public long Add(CompactKey key, long value)
        {
            key.ChangeDictionary(State.TreeDictionaryId);
            
            CompactTreeDumper.WriteAddition(this, key.Decoded(), value);

            AssertValueAndKeySize(key, value);

            FindPageFor(key, ref _internalCursor);
            
            // this overload assumes that a previous call to TryGetValue (where you go the encodedKey
            // already placed us in the right place for the value)
            Debug.Assert(_internalCursor._stk[_internalCursor._pos].Header->PageFlags == CompactPageFlags.Leaf,
                $"Got {_internalCursor._stk[_internalCursor._pos].Header->PageFlags} flag instead of {nameof(CompactPageFlags.Leaf)}");

            AddToPage(key, value);

            return key.ContainerId;
        }

        [SkipLocalsInit]
        private void AddToPage(CompactKey key, long value)
        {
            ref var state = ref _internalCursor._stk[_internalCursor._pos];
            
            state.Page = _llt.ModifyPage(state.Page.PageNumber);

            if (key.ContainerId == -1) // need to save this in the external terms container
            {
                var encodedKey = key.EncodedWithCurrent(out int encodedKeyLengthInBits);
                key.ContainerId = WriteTermToContainer(encodedKey, encodedKeyLengthInBits, key.Dictionary);
            }

            var entryBufferPtr = stackalloc byte[EncodingBufferSize];
            var requiredSize = EncodeEntry(EncodeKeyToPage(ref state, key.ContainerId), value, entryBufferPtr); 

            if (state.LastSearchPosition >= 0) // update
            {
                var len = GetEntrySize(ref state, state.LastSearchPosition);

                if (len == requiredSize)
                {
                    byte* entryPtr = state.Page.Pointer + state.EntriesOffsetsPtr[state.LastSearchPosition];
                    Unsafe.CopyBlockUnaligned(entryPtr, entryBufferPtr, (uint)requiredSize);
                    return;
                }

                // remove the entry, we'll need to add it as new
                int entriesCount = state.Header->NumberOfEntries;
                ushort* stateEntriesOffsetsPtr = state.EntriesOffsetsPtr;
                for (int i = state.LastSearchPosition; i < entriesCount - 1; i++)
                {
                    stateEntriesOffsetsPtr[i] = stateEntriesOffsetsPtr[i + 1];
                }

                state.Header->Lower -= sizeof(short);
                state.Header->FreeSpace += (ushort)(sizeof(short) + len);
                if (state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
                    _state.NumberOfEntries--; // we aren't counting branch entries
            }
            else
            {
                state.LastSearchPosition = ~state.LastSearchPosition;
                IncrementTermReferenceCount(key.ContainerId);
            }

            Debug.Assert(state.Header->FreeSpace >= (state.Header->Upper - state.Header->Lower));
            if (state.Header->Upper - state.Header->Lower < requiredSize + sizeof(short))
            {
                // we are *not* checking if we need to re-compress here, we already did with FindPageFor()
                bool splitAnyways = true;
                if (state.Header->FreeSpace >= requiredSize + sizeof(short) &&
                    // at the same time, we need to avoid spending too much time doing de-frags
                    // so we'll only do that when we have at least 1KB of free space to recover
                    state.Header->FreeSpace > Constants.Storage.PageSize / 8)
                {
                    DefragPage(_llt, ref state);
                    splitAnyways = state.Header->Upper - state.Header->Lower < requiredSize + sizeof(short);
                }

                if (splitAnyways)
                {
                    //DebugStuff.RenderAndShow(this);
                    SplitPage(key, value);
                    // a page split may cause us to do a search and reset the existing container id
                    //DebugStuff.RenderAndShow(this);
                   return;
                }
            }

            AddEntryToPage(key, state, requiredSize, entryBufferPtr);
            VerifySizeOf(ref state);
        }

        private void AddEntryToPage(CompactKey key, CursorState state, int requiredSize, byte* entryBufferPtr)
        {
            //VerifySizeOf(ref state);

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
  
            Unsafe.CopyBlockUnaligned(writePos, entryBufferPtr, (uint)requiredSize);
            newEntriesOffsets[state.LastSearchPosition] = state.Header->Upper;
            //VerifySizeOf(ref state);
        }

        private void SplitPage(CompactKey currentCauseForSplit, long valueForSplit)
        {
            if (_internalCursor._pos == 0) // need to create a root page
            {
                // We are going to be creating a root page with our first trained dictionary. 
                CreateRootPage();
            }

            // We create the new dictionary 
            ref var state = ref _internalCursor._stk[_internalCursor._pos];

            var page = _llt.AllocatePage(1);
            var header = (CompactPageHeader*)page.Pointer;
            header->PageFlags = state.Header->PageFlags;
            header->Lower = PageHeader.SizeOf ;
            header->Upper = Constants.Storage.PageSize;
            header->FreeSpace = Constants.Storage.PageSize - PageHeader.SizeOf;
            header->ContainerBasePage = ComputeContainerBasePage(currentCauseForSplit.ContainerId);
            
            if (header->PageFlags.HasFlag(CompactPageFlags.Branch))
            {
                _state.BranchPages++;
            }
            else
            {
                _state.LeafPages++;
            }

            // We may call SearchInCurrentPage and it will reset it, remember it for later
            long old = currentCauseForSplit.ContainerId;

            // We need to ensure that we have the correct key before we change the page. 
            var splitKey = SplitPageEncodedEntries(currentCauseForSplit, page, valueForSplit, ref state);

            PopPage(ref _internalCursor); // add to parent

            SearchInCurrentPage(splitKey, ref _internalCursor._stk[_internalCursor._pos]);
            
            currentCauseForSplit.ContainerId = old;
            
            AddToPage(splitKey, page.PageNumber);

            if (_internalCursor._stk[_internalCursor._pos].Header->PageFlags == CompactPageFlags.Leaf)
            {
                // we change the structure of the tree, so we can't reuse the state
                // but we can only do that as the _last_ part of the operation, otherwise
                // recursive split page will give bad results
                InitializeStateForTryGetNextValue(); 
            }

            VerifySizeOf(ref state);
        }

        private CompactKey SplitPageEncodedEntries(CompactKey causeForSplit, Page page, long valueForSplit, ref CursorState state)
        {
            var newPageState = new CursorState { Page = page };
            var entryBufferPtr = stackalloc byte[EncodingBufferSize];

            // sequential write up, no need to actually split
            int numberOfEntries = state.Header->NumberOfEntries;
            // this is lost on SearchInCurrentPage, so we need to keep the original value
            long causeForSplitContainerId = causeForSplit.ContainerId; 
            Debug.Assert(causeForSplitContainerId > 0);
            if (numberOfEntries == state.LastSearchPosition && state.LastMatch > 0)
            {
                newPageState.LastSearchPosition = 0; // add as first
                var entryLength = EncodeEntry(EncodeKeyToPage(ref newPageState, causeForSplitContainerId), 
                    valueForSplit, entryBufferPtr); 
                AddEntryToPage(causeForSplit, newPageState, entryLength, entryBufferPtr);
                return causeForSplit;
            }

            // non sequential write, let's just split in middle
            int entriesCopied = 0;
            int sizeCopied = 0;
            ushort* offsets = newPageState.EntriesOffsetsPtr;
            int i = FindPositionToSplitPageInHalfBasedOfEntriesSize(ref state);

            for (; i < numberOfEntries; i++)
            {
                GetEntryBuffer(ref state, i, out var entry);
                DecodeEntry(entry, out var keyDiff, out var val);
                var keyId = DecodeKeyFromPage(ref state, keyDiff);

                var entryLength = EncodeEntry(EncodeKeyToPage(ref newPageState, keyId), 
                    val, entryBufferPtr); 

                newPageState.Header->Lower += sizeof(ushort);
                newPageState.Header->Upper -= (ushort)entryLength;
                newPageState.Header->FreeSpace -= (ushort)(entryLength + sizeof(ushort));
                sizeCopied += entryLength + sizeof(ushort);
                offsets[entriesCopied++] = newPageState.Header->Upper;
                Memory.Copy(page.Pointer + newPageState.Header->Upper, entryBufferPtr, entryLength);
            }
            state.Header->Lower -= (ushort)(sizeof(ushort) * entriesCopied);
            state.Header->FreeSpace += (ushort)(sizeCopied);

            DefragPage(_llt, ref state); // need to ensure that we have enough space to add the new entry in the source page

            ref CursorState updatedPageState = ref newPageState; // start with the new page
            if (CompareEntryWith(ref state, state.Header->NumberOfEntries-1,causeForSplit) < 0)
            {
                // the new entry belong on the *old* page
                updatedPageState = ref state;
            }
            
            SearchInCurrentPage(causeForSplit, ref updatedPageState);
            Debug.Assert(updatedPageState.LastSearchPosition < 0, "There should be no updates here");
            updatedPageState.LastSearchPosition = ~updatedPageState.LastSearchPosition;
            {
                var entryLength = EncodeEntry(EncodeKeyToPage(ref updatedPageState, causeForSplitContainerId), 
                    valueForSplit, entryBufferPtr); 
                Debug.Assert(updatedPageState.Header->Upper - updatedPageState.Header->Lower >= entryLength);
                AddEntryToPage(causeForSplit, updatedPageState, entryLength, entryBufferPtr);
            }
            VerifySizeOf(ref newPageState);
            VerifySizeOf(ref state);
            
            var splitKeySpan= GetEncodedKeySpan(ref newPageState, 0,out var splitKeyLengthInBits);

            var updateCauseForSplit = new CompactKeyCacheScope(_llt, splitKeyLengthInBits, splitKeySpan, State.TreeDictionaryId);
            return updateCauseForSplit.Key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private int CompareEntryWith(ref CursorState state, int position, CompactKey encodedKey)
        {
            long containerDiff = DecodeKey(state.Page.Pointer + state.EntriesOffsetsPtr[position]);
            var containerId = DecodeKeyFromPage(ref state, containerDiff);
            GetEncodedKeyPtr(containerId, out var lastEntryFromPreviousPage, out _, out var sizeInBits);
            return encodedKey.CompareEncodedWithCurrent(lastEntryFromPreviousPage, sizeInBits);
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
                var len = GetEntrySize(ref state, i);
                sizeUsed += len;
                if (sizeUsed >= halfwaySizeMark)
                    return i;
            }
            // we should never reach here, but let's have a reasonable default
            Debug.Assert(false, "How did we reach here?");
            return numberOfEntries / 2;
        }

        [Conditional("DEBUG")]
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

            if (p.Header->Upper - p.Header->Lower < 0)
            {
                throw new InvalidOperationException($"Lower {p.Header->Lower} > Upper {p.Header->Upper}");
            }

            if (p.Header->FreeSpace > Constants.Storage.PageSize - PageHeader.SizeOf)
            {
                throw new InvalidOperationException($"FreeSpace is too large: {p.Header->FreeSpace}");
            }

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

        private HashSet<long> _pagesThatWeFailedToRecompress;

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
            state.Header->ContainerBasePage = Container.GetNextFreePage(_llt, State.TermsContainerId) * Constants.Storage.PageSize;
            
            var pageNumberBufferPtr = stackalloc byte[EncodingBufferSize];
            var size = EncodeEntry(EncodeKeyToPage(ref state, 0), cpy, pageNumberBufferPtr);

            state.Header->Upper = (ushort)(Constants.Storage.PageSize - size);
            state.Header->FreeSpace -= (ushort)(size + sizeof(ushort));

            state.EntriesOffsetsPtr[0] = state.Header->Upper;
            byte* entryPos = state.Page.Pointer + state.Header->Upper;

            // This is a zero length key (which is the start of the tree).
            Memory.Copy(entryPos, pageNumberBufferPtr, size);

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
            using (llt.GetTempPage(Constants.Storage.PageSize, out var tmpPage))
            {
                // Ensure we clean up the page.               
                var tmpPtr = tmpPage.Base;
                Unsafe.InitBlock(tmpPtr, 0, Constants.Storage.PageSize);

                // We copy just the header and start working from there.
                var tmpHeader = (CompactPageHeader*)tmpPtr;
                *tmpHeader = *(CompactPageHeader*)state.Page.Pointer;
                                
                Debug.Assert(tmpHeader->Upper - tmpHeader->Lower >= 0);
                Debug.Assert(tmpHeader->FreeSpace <= Constants.Storage.PageSize - PageHeader.SizeOf);
                
                // We reset the data pointer                
                tmpHeader->Upper = Constants.Storage.PageSize;

                var tmpEntriesOffsets = new Span<ushort>(tmpPtr + PageHeader.SizeOf, state.Header->NumberOfEntries);

                // For each entry in the source page, we copy it to the temporary page.
                var sourceEntriesOffsets = state.EntriesOffsets;
                for (int i = 0; i < sourceEntriesOffsets.Length; i++)
                {
                    // Retrieve the entry data from the source page
                    var len = GetEntryBuffer(ref state, i, out var entryBuffer);
                    Debug.Assert((tmpHeader->Upper - len) > 0);

                    ushort lowerIndex = (ushort)(tmpHeader->Upper - len);

                    // Note: Since we are just defragmenting, FreeSpace doesn't change.
                    Unsafe.CopyBlockUnaligned(tmpPtr + lowerIndex, entryBuffer, (uint)len);

                    tmpEntriesOffsets[i] = lowerIndex;
                    tmpHeader->Upper = lowerIndex;
                }
                // We have consolidated everything therefore we need to update the new free space value.
                tmpHeader->FreeSpace = (ushort)(tmpHeader->Upper - tmpHeader->Lower);

                // We copy back the defragmented structure on the temporary page to the actual page.
                Unsafe.CopyBlockUnaligned(state.Page.Pointer, tmpPtr, Constants.Storage.PageSize);
                Debug.Assert(state.Header->FreeSpace == (state.Header->Upper - state.Header->Lower));
            }
        }

        public List<(string, long)> AllEntriesIn(long p)
        {
            Page page = _llt.GetPage(p);
            var state = new CursorState { Page = page, };

            var results = new List<(string, long)>();

            using var scope = new CompactKeyCacheScope(this._llt);
            for (ushort i = 0; i < state.Header->NumberOfEntries; i++)
            {
                var encodedKey = GetEncodedKeySpan(ref state, i, out var encodedKeyLengthInBits);
                scope.Key.Set(encodedKeyLengthInBits, encodedKey, State.TreeDictionaryId);

                var val = GetValue(ref state, i);
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
            using var scope = new CompactKeyCacheScope(this._llt);
            var encodedKey = scope.Key;
            encodedKey.Set(key);
            encodedKey.ChangeDictionary(State.TreeDictionaryId);
            FindPageFor(encodedKey, ref cstate);
        }

        private void FindPageFor(CompactKey key, ref IteratorCursorState cstate)
        {
            cstate._pos = -1;
            cstate._len = 0;
            PushPage(_state.RootPage, ref cstate);

            ref var state = ref cstate._stk[cstate._pos];

            FindPageFor(ref cstate, ref state, key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void FindPageFor(ref IteratorCursorState cstate, ref CursorState state, CompactKey compactKey)
        {
            while (state.Header->IsBranch)
            {
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
            SearchInCurrentPage(compactKey, ref state);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long GetValue(ref CursorState nState, int actualPos)
        {
            return DecodeValue(nState.Page.Pointer + nState.EntriesOffsetsPtr[actualPos]);
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

        internal static bool GetEntry(CompactTree tree, ref CursorState state, int pos, out CompactKeyCacheScope key, out long value)
        {
            var encodedKeyStream = tree.GetEncodedKeySpan(ref state, pos, out var encodedKeyLengthInBits, out value);
            if (encodedKeyStream.Length == 0)
            {
                key = default;
                return false;
            }

            key = new CompactKeyCacheScope(tree._llt, encodedKeyLengthInBits, encodedKeyStream, tree.State.TreeDictionaryId);

            return true;
        }
        
        private void SearchInCurrentPage(CompactKey key, ref CursorState state)
        {
            Debug.Assert(state.Header->Upper - state.Header->Lower >= 0);
            Debug.Assert(state.Header->FreeSpace <= Constants.Storage.PageSize - PageHeader.SizeOf);

            ushort* @base = state.EntriesOffsetsPtr;
            int length = state.Header->NumberOfEntries;
            int bot = 0;
            int match = -1;
            if (length == 0)
            {
                goto NotFound;
            }

            byte* keyPtr = key.EncodedWithPtr(key.Dictionary, out var keyLengthInBits);
            int keyLength = Bits.ToBytes(keyLengthInBits);

            int top = length;
            
            while (top > 1)
            {
                int mid = top / 2;

                var midKeyDiff = DecodeKey(state.Page.Pointer + @base[bot + mid]);
                var midKeyId = DecodeKeyFromPage(ref state, midKeyDiff);
                match = CompareKeys(midKeyId, keyPtr, keyLength, keyLengthInBits);

                if (match >= 0)
                    bot += mid;

                top -= mid;
            }

            long currentKeyDiff = DecodeKey(state.Page.Pointer + @base[bot]);
            var currentKeyId = DecodeKeyFromPage(ref state, currentKeyDiff);
            match = CompareKeys(currentKeyId, keyPtr, keyLength, keyLengthInBits);
            if (match == 0)
            {
                key.ContainerId = currentKeyId;
                state.LastMatch = 0;
                state.LastSearchPosition = bot;
                return;
            }

            NotFound:
            key.ContainerId = -1;
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
            var encodedKey = key.EncodedWithCurrent(out var _);

            int high = state.Header->NumberOfEntries - 1, low = 0;
            int match = -1;
            int mid = 0;
            while (low <= high)
            {
                mid = (high + low) / 2;
                
                var currentKey = GetEncodedKeySpan(ref state, mid, out _);

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
        }

        private void FuzzyFindPageFor(ReadOnlySpan<byte> key, ref IteratorCursorState cstate)
        {
            // Algorithm 2: Find Node

            cstate._pos = -1;
            cstate._len = 0;
            PushPage(_state.RootPage, ref cstate);

            ref var state = ref cstate._stk[cstate._pos];

            using var scope = new CompactKeyCacheScope(this._llt, key, State.TreeDictionaryId);
            
            var encodedKey = scope.Key;

            while (state.Header->IsBranch)
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
