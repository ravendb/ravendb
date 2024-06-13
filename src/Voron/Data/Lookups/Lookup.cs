using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Sparrow;
using Sparrow.Server;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Debugging;
using Voron.Global;
using Voron.Impl;
namespace Voron.Data.Lookups;

public sealed unsafe partial class Lookup<TLookupKey> : IPrepareForCommit
    where TLookupKey : struct,  ILookupKey
{
    
    private const int EncodingBufferSize = sizeof(long) + sizeof(long) + 1;
    
    private LowLevelTransaction _llt;
    private LookupState _state;
    private int _treeStructureVersion;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int EncodeEntry(LookupPageHeader* header, long keyData, long val, byte* buffer)
    {
        var k = ZigZag(keyData- header->KeysBase);
        var v = ZigZag(val - header->ValuesBase);
        var keyLen = 8 - BitOperations.LeadingZeroCount((ulong)k) / 8;
        var valLen = 8 - BitOperations.LeadingZeroCount((ulong)v) / 8;
        Debug.Assert(keyLen <= 8 && valLen <= 8);

        ref var bufferPtr = ref Unsafe.AsRef<byte>(buffer);
        Unsafe.WriteUnaligned(ref bufferPtr, (byte)(keyLen << 4 | valLen));
        Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref bufferPtr, 1), k);
        Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref bufferPtr, 1 + keyLen), v);

        return 1 + keyLen + valLen;
    }
    
    // the diff may be *negative*, so we use zig/zag encoding to handle this
    private static long ZigZag(long diff) => (diff << 1) ^ (diff >> 63);
    

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int DecodeEntry(ref CursorState state, int pos, out long keyData, out long val)
    {
        Debug.Assert(pos >= 0 && pos < state.Header->NumberOfEntries);
        var  buffer = state.Page.Pointer + state.EntriesOffsetsPtr[pos];
        return DecodeEntry(state.Header, buffer, out keyData, out val);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int DecodeEntry(LookupPageHeader* header, int pos, out long keyData, out long val)
    {
        Debug.Assert(pos >= 0 && pos < header->NumberOfEntries);
        ushort* entries = (ushort*)((byte*)header + PageHeader.SizeOf);
        var  buffer = (byte*)header + entries[pos];
        return DecodeEntry(header, buffer, out keyData, out val);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int DecodeEntry(LookupPageHeader* header, byte* buffer, out long keyData, out long val)
    {
        var keyLen = buffer[0] >> 4;
        var valLen = buffer[0] & 0xF;
        long k = 0, v = 0;
        Memory.Copy(&k, buffer + 1, keyLen);
        Memory.Copy(&v, buffer + 1 + keyLen, valLen);
        keyData = Unzag(k, header->KeysBase);
        val = Unzag(v, header->ValuesBase);
        return 1 + keyLen + valLen;

    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long Unzag(long l, long baseline)
    {
        long unzag = (l & 1) == 0 ? l >>> 1 : (l >>> 1) ^ -1;
        return unzag + baseline;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetEntryBuffer(ref CursorState state, int pos, out byte* buffer)
    {
        Debug.Assert(pos >= 0 && pos < state.Header->NumberOfEntries);
        buffer = state.Page.Pointer + state.EntriesOffsetsPtr[pos];
        var keyLen = buffer[0] >> 4;
        var valLen = buffer[0] & 0xF;
        return keyLen + valLen + 1;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetEntryBuffer(LookupPageHeader* p, int pos, out byte* buffer)
    {
        Debug.Assert(pos >= 0 && pos < p->NumberOfEntries);
        ushort* entries = (ushort*)((byte*)p + PageHeader.SizeOf);
        buffer = (byte*)p + entries[pos];
        var keyLen = buffer[0] >> 4;
        var valLen = buffer[0] & 0xF;
        return keyLen + valLen + 1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetEntrySize(ref CursorState state, int pos)
    {
        Debug.Assert(pos >= 0 && pos < state.Header->NumberOfEntries);
        var buffer = state.Page.Pointer + state.EntriesOffsetsPtr[pos];
        return GetEntrySize(buffer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetEntrySize(byte* buffer)
    {
        var keyLen = buffer[0] >> 4;
        var valLen = buffer[0] & 0xF;
        return keyLen + valLen + 1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long GetKeyData(ref CursorState state, int pos)
    {
        Debug.Assert(pos >= 0 && pos < state.Header->NumberOfEntries);
        var buffer = state.Page.Pointer + state.EntriesOffsetsPtr[pos];
        return GetKeyData(state.Header, buffer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetKeyData(LookupPageHeader* header, byte* buffer)
    {
        var keyLen = buffer[0] >> 4;
        long k = ReadBackward(buffer + 1, keyLen);

        k = Unzag(k, header->KeysBase);
        return k;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ReadBackward(byte* b, int len)
    {
        if (len == 0)
            return 0;
        var shift = 8 - len;
        // this is safe to do, we are always *at least* 64 bytes from the page header
        var l = Unsafe.ReadUnaligned<long>(ref b[-shift]);
        l >>>= shift * 8;
        return l;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetValue(ref CursorState state, int pos)
    {
        Debug.Assert(pos >= 0 && pos < state.Header->NumberOfEntries);
        var buffer = state.Page.Pointer + state.EntriesOffsetsPtr[pos];
        var keyLen = buffer[0] >> 4;
        var valLen = buffer[0] & 0xF;
        long v = ReadBackward(buffer + 1 + keyLen, valLen);

        return Unzag(v, state.Header->ValuesBase);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetKeyAndValue(ref CursorState state, int pos, out long key, out long value)
    {
        Debug.Assert(pos >= 0 && pos < state.Header->NumberOfEntries);
        var buffer = state.Page.Pointer + state.EntriesOffsetsPtr[pos];
        var keyLen = buffer[0] >> 4;
        var valLen = buffer[0] & 0xF;
        long k = ReadBackward(buffer + 1, keyLen);
        long v = ReadBackward(buffer + 1 + keyLen, valLen);
        key = Unzag(k, state.Header->KeysBase);
        value = Unzag(v, state.Header->ValuesBase);

        return keyLen + valLen + 1;
    }

    private struct IteratorCursorState
    {
        internal CursorState[] _stk;
        internal int _pos;
        internal int _len;
    }

    // TODO: Improve interactions with caller code. It is good enough for now but we can encapsulate behavior better to improve readability. 
    private IteratorCursorState _internalCursor = new() { _stk = new CursorState[8], _pos = -1, _len = 0 };

    public struct CursorState
    {
        public Page Page;
        public int LastMatch;
        public int LastSearchPosition;

        public LookupPageHeader* Header => (LookupPageHeader*)Page.Pointer;

        public Span<ushort> EntriesOffsets => new Span<ushort>(Page.Pointer + PageHeader.SizeOf, Header->NumberOfEntries);
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
    
    public static Lookup<TLookupKey> Open(LowLevelTransaction llt,LookupState state)
    {
        return new Lookup<TLookupKey> { _llt = llt, _state = state };
    }

    private Lookup()
    {

    }
    
    private Lookup(Slice name, Tree parent)
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
    public ref LookupState State => ref _state;
    public LowLevelTransaction Llt => _llt;

    public static Lookup<TLookupKey> InternalCreate(Tree parent, Slice name, long dictionaryId = -1, long termsContainerId = -1)
    {
        var llt = parent.Llt;

        LookupState header;

        var existing = parent.Read(name);
        if (existing == null)
        {
            if (llt.Flags != TransactionFlags.ReadWrite)
                return null;

            if ((parent.State.Header.Flags & TreeFlags.Lookups) != TreeFlags.Lookups)
            {
                ref var state = ref parent.State.Modify();
                state.Flags |= TreeFlags.Lookups;
            }

            Create(llt, out header, dictionaryId, termsContainerId);
            using var _ = parent.DirectAdd(name, sizeof(LookupState), out var p);
            *(LookupState*)p = header;
        }
        else
        {
            header = *(LookupState*)existing.Reader.Base;
        }

        if (header.RootObjectType != RootObjectType.Lookup)
            throw new InvalidOperationException($"Tried to open {name} as a lookup, but it is actually a {header.RootObjectType}");

        return new Lookup<TLookupKey>(name, parent)
        {
            _llt = llt,
            _state = header
        };
    }
    
    public static void Create(LowLevelTransaction llt, out LookupState state, long dictionaryId = -1, long termsContainerId = -1)
    {
        var newPage = llt.AllocatePage(1);
        var compactPageHeader = (LookupPageHeader*)newPage.Pointer;
        compactPageHeader->PageFlags = LookupPageFlags.Leaf;
        compactPageHeader->Lower = PageHeader.SizeOf;
        compactPageHeader->Upper = Constants.Storage.PageSize;
        compactPageHeader->FreeSpace = Constants.Storage.PageSize - (PageHeader.SizeOf);
        compactPageHeader->KeysBase = newPage.PageNumber;
        compactPageHeader->ValuesBase = newPage.PageNumber;
        state = new LookupState
        {
            RootObjectType = RootObjectType.Lookup,
            BranchPages = 0,
            LeafPages = 1,
            RootPage = newPage.PageNumber,
            NumberOfEntries = 0,
            DictionaryId = dictionaryId,
            TermsContainerId = termsContainerId
        };
    }

    
    public void PrepareForCommit()
    {
        using var _ = _parent.DirectAdd(Name, sizeof(LookupState), out var ptr);
        _state.CopyTo((LookupState*)ptr);
    }

    public bool TryGetValue(TLookupKey key, out long value) => TryGetValue(ref key, out value);

    public bool TryGetTermContainerId(TLookupKey key, out long value)
    {
        if (typeof(TLookupKey) != typeof(CompactTree.CompactKeyLookup))
            throw new NotSupportedException(typeof(TLookupKey).FullName);
        
        FindPageFor(ref key, ref _internalCursor);
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        if (state.LastMatch != 0)
        {
            value = -1;
            return false;
        }

        //For CompactTree this is external term container.
        value = GetKeyData(ref state, state.LastSearchPosition);
        return true;
    }
    
    public bool TryGetValue(ref TLookupKey key, out long value)
    {
        FindPageFor(ref key, ref _internalCursor);
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        if (state.LastMatch != 0)
        {
            value = -1;
            return false;
        }

        value = GetValue(ref state, state.LastSearchPosition);
        return true;
    }

    public bool TryRemove(TLookupKey key) => TryRemove(ref key);

    public bool TryRemove(ref TLookupKey key)
    {
        FindPageFor(ref key, ref _internalCursor);
        return RemoveFromPage(ref key, allowRecurse: true, isExplicitRemove: true);
    }

    public bool TryRemove(TLookupKey key, out long value) => TryRemove(ref key, out value);
    
    public bool TryRemove(ref TLookupKey key, out long value)
    {
        FindPageFor(ref key, ref _internalCursor);
        if (_internalCursor._stk[_internalCursor._pos].LastMatch != 0)
        {
            value = -1;
            return false;
        }
        return TryRemoveExistingValue(ref key, out value);
    }

    public bool TryRemoveExistingValue(ref TLookupKey key, out long value)
    {
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        value = GetValue(ref state, state.LastSearchPosition);
        return RemoveFromPage(ref key, allowRecurse: true, isExplicitRemove: true);
    }

    private void RemoveFromPage(bool allowRecurse, int pos, bool isExplicitRemove)
    {
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        state.LastSearchPosition = pos;
        state.LastMatch = 0;
        long keyData = GetKeyData(ref state, pos);
        var key = TLookupKey.FromLong<TLookupKey>(keyData);
        RemoveFromPage(ref key, allowRecurse, isExplicitRemove);
    }

    private bool RemoveFromPage(ref TLookupKey key, bool allowRecurse, bool isExplicitRemove)
    {
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        if (state.LastMatch != 0)
        {
            return false;
        }

        state.Page = _llt.ModifyPage(state.Page.PageNumber);

        RemoveEntryFromPage(ref state, ref key, state.LastSearchPosition, isExplicitRemove, out _);

        if (state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf))
        {
            _state.NumberOfEntries--;
        }

        if (allowRecurse &&
            _internalCursor._pos > 0 && // nothing to do for a single leaf node
            state.Header->FreeSpace > Constants.Storage.PageSize / 2)
        {
            if (MaybeMergeEntries(ref state))
            {
                VerifySizeOfFullCursor();
                _treeStructureVersion++;
                InitializeCursorState(); // we change the structure of the tree, so we can't reuse 
            }
        }

        VerifySizeOf(ref state, _internalCursor._pos == 0);
        return true;
    }
    
    private void RemoveEntryFromPage(ref CursorState state, ref TLookupKey key, int pos, bool isExplicitRemove, out long value)
    {
        var entriesOffsets = state.EntriesOffsets;
        var len = GetKeyAndValue(ref state, pos, out var k, out value);

        state.Header->FreeSpace += (ushort)(sizeof(ushort) + len);
        state.Header->Lower -= sizeof(short); // the upper will be fixed on defrag

        Debug.Assert(state.Header->Upper - state.Header->Lower >= 0);
        Debug.Assert(state.Header->FreeSpace <= Constants.Storage.PageSize - PageHeader.SizeOf);
        Debug.Assert(k == key.ToLong(), "k == key.ToLong()");
        
        if (state.Header->IsBranch || isExplicitRemove)
            key.OnKeyRemoval(this);
        
        entriesOffsets[(pos + 1)..].CopyTo(entriesOffsets[pos..]);
    }

    [Conditional("DEBUG")]
    private void VerifySizeOfFullCursor()
    {
        for (int i = 0; i < _internalCursor._pos; i++)
        {
            VerifySizeOf(ref _internalCursor._stk[i], i == 0);
        }
    }

    private bool MaybeMergeEntries(ref CursorState destinationState)
    {
        Debug.Assert(_internalCursor._pos > 0, "_internalCursor._pos > 0");
        ref var parent = ref _internalCursor._stk[_internalCursor._pos - 1];
        parent.Page = _llt.ModifyPage(parent.Page.PageNumber);
        Debug.Assert(parent.Header->NumberOfEntries >= 2,"parent.Header->NumberOfEntries >= 2");

        if (destinationState.Header->NumberOfEntries == 0)
        {
            RemovePageFromParent(ref destinationState, ref parent);
            return true;
        }

        if (parent.LastSearchPosition == 0 || parent.LastSearchPosition == parent.Header->NumberOfEntries - 1)
        {
            // optimization: not merging right most / left most pages that allows to delete in up / down order without doing any
            // merges, for FIFO / LIFO scenarios
            return false;
        }

        var siblingPage = GetValue(ref parent, parent.LastSearchPosition + 1);
        var sourceState = new CursorState
        {
            Page = _llt.ModifyPage(siblingPage)
        };

        if (sourceState.Header->PageFlags != destinationState.Header->PageFlags)
            return false; // cannot merge leaf & branch pages

        int combinedFreeSpace = sourceState.Header->FreeSpace + destinationState.Header->FreeSpace;
        if (combinedFreeSpace <= Constants.Storage.PageSize)
        {
            // no point in trying if after combining *both* pages, we'll be completely full
            // we also want to avoid merge / split cycles - so a merge should happen only when
            // there is sufficient free space _after_ the merge  
            return false;
        }
        
        DefragPage(_llt, ref destinationState);

        // we have to use a temporary copy, because we may fail to copy _all_ the values from the nearby page
        using var _ = _llt.Allocator.Allocate(Constants.Storage.PageSize, out ByteString temp);
        Memory.Copy(temp.Ptr, destinationState.Page.Pointer, Constants.Storage.PageSize);

        if (TryMoveAllEntries((LookupPageHeader*)temp.Ptr, sourceState.Header) == false)
            return false; // nothing to be done here, we cannot fully merge, so abort
        
        // now copy from the temp buffer to the actual page
        Debug.Assert(_llt.IsDirty(destinationState.Page.PageNumber));
        Memory.Copy(destinationState.Page.Pointer, temp.Ptr, Constants.Storage.PageSize);
        // We update the entries offsets on the source page, now that we have moved the entries.
        parent.LastSearchPosition++;
        FreePageFor(ref destinationState, ref sourceState, ref parent);
        return true;

    }
    
    [SkipLocalsInit]
    private bool TryMoveAllEntries(LookupPageHeader* destination, LookupPageHeader* source)
    {
        // PERF: This method is marked SkipLocalInit because we want to avoid initialize these values
        // as we are going to be writing them anyways.
        byte* entryBuffer = stackalloc byte[EncodingBufferSize];

        bool reEncode = source->KeysBase != destination->KeysBase ||
                        source->ValuesBase != destination->ValuesBase;
        // the new entries size is composed of entries from _both_ pages            
        int combinedEntries = destination->NumberOfEntries + source->NumberOfEntries;
        var entries = new Span<ushort>((byte*)destination + PageHeader.SizeOf, combinedEntries)[destination->NumberOfEntries..];
        int idx = 0;
        long firstItemKey = -1;        
        if (destination->IsBranch)
        {
            // special handling for the left most section
            (firstItemKey, long childValue) = GetFirstActualKeyAndValue(source);
            var len = EncodeEntry(destination, firstItemKey, childValue, entryBuffer);
            if (AddBufferToPage(entryBuffer, len, ref entries[idx]) == false)
                return false;
            
            idx++;
        }
        for (; idx < source->NumberOfEntries; idx++)
        {
            int requiredSize;
            byte* buffer;
            if (reEncode)
            {
                //We get the encoded key and value from the sibling page
                DecodeEntry(source, idx, out var key, out var value);
                requiredSize = EncodeEntry(destination, key, value, entryBuffer);
                buffer = entryBuffer;
            }
            else
            {
                // We get the encoded key and value from the sibling page
                requiredSize = GetEntryBuffer(source, idx, out buffer);
            }

            if (AddBufferToPage(buffer, requiredSize, ref entries[idx]) == false)
                return false;
        }

        if (destination->IsBranch)
        {
            var firstItemLookupKey = TLookupKey.FromLong<TLookupKey>(firstItemKey);
            firstItemLookupKey.OnNewKeyAddition(this);
        }

        return true;
        
        bool AddBufferToPage(byte* buffer, int requiredSize, ref ushort entry)
        {
            if (requiredSize + sizeof(ushort) > destination->Upper - destination->Lower)
            {
                return false; // failed to move
            }

            // We will update the entries offsets in the receiving page.
            destination->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
            destination->Upper -= (ushort)requiredSize;
            destination->Lower += sizeof(ushort);
            entry = destination->Upper;

            var entryPos = (byte*)destination + destination->Upper;
            Memory.Copy(entryPos, buffer, requiredSize);

            Debug.Assert(destination->Upper >= destination->Lower);
            
            return true;
        }
    }
    
    private (long key, long childValue) GetFirstActualKeyAndValue(LookupPageHeader* src)
    {
        Debug.Assert(src->IsBranch,"destination->IsBranch");

        DecodeEntry(src, 0, out _, out var childValue);

        var child = src;
        while (child->IsBranch)
        {
            DecodeEntry(src, 0, out _, out var pg);
            var childPage = _llt.GetPage(pg);
            child = (LookupPageHeader*)childPage.Pointer;
        }
        DecodeEntry(child, 0, out var key, out _);

        // important, we go _down_ the tree until we find the value from the leaf page
        // note that the *key* value changes and we return the _bottom most_ key with the _first_ value
        return (key, childValue);
    }


    private void RemovePageFromParent(ref CursorState destinationState, ref CursorState parent)
    {
        
        // can just remove the whole thing
        int position = parent.LastSearchPosition == 0 ? 1 : parent.LastSearchPosition - 1;
        if (position < 0 || position >= parent.Header->NumberOfEntries)
            throw new ArgumentOutOfRangeException(
                $"Found a parent page {parent.Page.PageNumber} where the number of entries is invalid ({parent.Header->NumberOfEntries}) since position is ({position})");

        var sibling = GetValue(ref parent, position);
        var sourceState = new CursorState { Page = _llt.GetPage(sibling) };
        Debug.Assert(sourceState.Header->IsBranch ^ sourceState.Header->IsLeaf, "sourceState.Header->IsBranch ^ sourceState.Header->IsLeaf");
        if (parent.LastSearchPosition > 0)
        {
            FreePageFor(ref sourceState, ref destinationState, ref parent);
            return;
        }

        // if we are removing the leftmost item, we need to maintain the smallest entry, just copy the sibling's contents
        long pageNum = destinationState.Page.PageNumber;
        Debug.Assert(_llt.IsDirty(destinationState.Page.PageNumber));
        Memory.Copy(destinationState.Page.Pointer, sourceState.Page.Pointer, Constants.Storage.PageSize);
        destinationState.Page.PageNumber = pageNum;

        // now ask that we'll remove the _sibling_ page, not us, since we copied it
        parent.LastSearchPosition++;
        FreePageFor(ref destinationState, ref sourceState, ref parent);
    }

    private void FreePageFor(ref CursorState stateToKeep, ref CursorState stateToDelete, ref CursorState parent)
    {
        DecrementPageNumbers(ref stateToDelete);
        Debug.Assert(parent.Header->NumberOfEntries >= 2, "parent.Header->NumberOfEntries >= 2");
        if (parent.Header->NumberOfEntries == 2)
        {
            // let's reduce the height of the tree entirely...
            DecrementPageNumbers(ref parent);

            var parentPageNumber = parent.Page.PageNumber;
            Debug.Assert(_llt.IsDirty(parent.Page.PageNumber));
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
            RemoveFromPage(allowRecurse: true, parent.LastSearchPosition, isExplicitRemove: false);
        }
    }

    private void DecrementPageNumbers(ref CursorState state)
    {
        if (state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf))
        {
            _state.LeafPages--;
        }
        else
        {
            _state.BranchPages--;
        }
    }

    public void Add(ref TLookupKey key, long value)
    {
        FindPageFor(ref key, ref _internalCursor);

        AddToPage(ref key, value);
    }

    public void AddAfterTryGetNext(ref TLookupKey key, long value)
    {        
        Debug.Assert(_internalCursor._stk[_internalCursor._pos].LastSearchPosition < 0);
        AddToPage(ref key, value);
    }
    
    public void SetAfterTryGetNext(ref TLookupKey key, long value)
    {        
        Debug.Assert(_internalCursor._stk[_internalCursor._pos].LastSearchPosition >= 0);
        AddToPage(ref key, value);
    }

    public void AddOrSetAfterGetNext(ref TLookupKey key, long value)
    {
        AddToPage(ref key, value);
    }
    
    public void Add(TLookupKey key, long value)
    {
        Add(ref key, value);
    }

    [SkipLocalsInit]
    private void AddToPage(ref TLookupKey key, long value, bool searchForKeyOnSplit = false)
    {
        ref var state = ref _internalCursor._stk[_internalCursor._pos];

        state.Page = _llt.ModifyPage(state.Page.PageNumber);

        bool isUpdate = true;
        if (state.LastSearchPosition < 0)
        {
            isUpdate = false;
            state.LastSearchPosition = ~state.LastSearchPosition;
            key.OnNewKeyAddition(this);
        }
        Debug.Assert(state.LastSearchPosition <= state.Header->NumberOfEntries, "state.LastSearchPosition <= state.Header->NumberOfEntries");

        var entryBufferPtr = stackalloc byte[EncodingBufferSize];
        var requiredSize = EncodeEntry(state.Header, key.ToLong(), value, entryBufferPtr);

        if (isUpdate)
        {
            var len = GetEntrySize(ref state, state.LastSearchPosition);

            if (len == requiredSize)
            {
                byte* entryPtr = state.Page.Pointer + state.EntriesOffsetsPtr[state.LastSearchPosition];
                Unsafe.CopyBlockUnaligned(entryPtr, entryBufferPtr, (uint)requiredSize);
                return;
            }

            // mark the entry as invalid, we'll need to set it later
            state.EntriesOffsetsPtr[state.LastSearchPosition] = ushort.MaxValue;

            state.Header->Lower -= sizeof(short);
            state.Header->FreeSpace += (ushort)(sizeof(short) + len);
            if (state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf))
                _state.NumberOfEntries--; // we aren't counting branch entries
        }

        Debug.Assert(state.Header->FreeSpace >= (state.Header->Upper - state.Header->Lower));
        if (state.Header->Upper - state.Header->Lower < requiredSize + sizeof(short))
        {
            if (isUpdate)
            {
                // we put an *invalid* marker in there (to avoid moving entries back & forth)
                // but now we need to defrag or split, so let's remove it

                var src = new Span<ushort>(state.EntriesOffsetsPtr + state.LastSearchPosition + 1,
                    // not reducing the count here because the number of elements here was reduced the count previously
                    state.Header->NumberOfEntries - state.LastSearchPosition);
                var dst = new Span<ushort>(state.EntriesOffsetsPtr + state.LastSearchPosition, 
                    state.Header->NumberOfEntries - state.LastSearchPosition);
                src.CopyTo(dst);
                isUpdate = false; // so the follow up on AddEntryToPage() will know to move it back
            }
            
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
                if (searchForKeyOnSplit)
                {
                    // we have to do this if we are being called from BulkUpdate, since
                    // we don't remember the LastMatch, and that _is_ being used during the 
                    // splitting process to direct the shape of the tree
                    SearchInCurrentPage(ref key, ref state);
                    if (state.LastSearchPosition < 0)
                        state.LastSearchPosition = ~state.LastSearchPosition;
                }
                SplitPage(ref key, value);
                VerifySizeOfFullCursor();
                return;
            }
        }

        AddEntryToPage(ref state, requiredSize, entryBufferPtr, isUpdate);
        VerifySizeOf(ref state, _internalCursor._pos == 0);
    }

    private void AddEntryToPage(ref CursorState state, int requiredSize, byte* entryBufferPtr, bool isUpdate)
    {
        //VerifySizeOf(ref state);

        var header = state.Header;

        header->Lower += sizeof(short);
        var newNumberOfEntries = header->NumberOfEntries;

        ushort* newEntriesStartPtr = state.EntriesOffsetsPtr + newNumberOfEntries - 1;
        ushort* newEntriesEndPtr = state.EntriesOffsetsPtr + state.LastSearchPosition;

        // for updates, we put an invalid marker, and don't need to shift the list of offsets twice
        if (isUpdate == false)
        {
            if (Vector256.IsHardwareAccelerated)
            {
                int N256 = (Vector256<ushort>.Count - 1);
                while (newEntriesStartPtr - Vector256<ushort>.Count >= newEntriesEndPtr)
                {
                    var source = Vector256.Load(newEntriesStartPtr - N256 - 1);
                    Vector256.Store(source, newEntriesStartPtr - N256);
                    newEntriesStartPtr -= Vector256<ushort>.Count;
                }
            }

            if (Vector128.IsHardwareAccelerated)
            {
                int N128 = (Vector128<ushort>.Count - 1);
                while (newEntriesStartPtr - Vector128<ushort>.Count >= newEntriesEndPtr)
                {
                    var source = Vector128.Load(newEntriesStartPtr - N128 - 1);
                    Vector128.Store(source, newEntriesStartPtr - N128);
                    newEntriesStartPtr -= Vector128<ushort>.Count;
                }
            }

            // This will move one element at a time... the worst case scenario.
            while (newEntriesStartPtr >= newEntriesEndPtr)
            {
                *newEntriesStartPtr = *(newEntriesStartPtr - 1);
                newEntriesStartPtr--;
            }

        }
        
        if (header->PageFlags.HasFlag(LookupPageFlags.Leaf))
            _state.NumberOfEntries++; // we aren't counting branch entries

        Debug.Assert(header->FreeSpace >= requiredSize + sizeof(ushort));

        header->FreeSpace -= (ushort)(requiredSize + sizeof(ushort));
        header->Upper -= (ushort)requiredSize;

        // We are going to be storing in the following format:
        // [ keySizeInBits: ushort | key: sequence<byte> | value: varint ]
        byte* writePos = state.Page.Pointer + header->Upper;

        Unsafe.CopyBlockUnaligned(writePos, entryBufferPtr, (uint)requiredSize);
        
        state.EntriesOffsetsPtr[state.LastSearchPosition] = header->Upper;
        //VerifySizeOf(ref state);
    }

    private void SplitPage(ref TLookupKey currentCauseForSplit, long valueForSplit)
    {
        _treeStructureVersion++;
        
        if (_internalCursor._pos == 0) // need to create a root page
        {
            // We are going to be creating a root page with our first trained dictionary. 
            CreateRootPage(ref currentCauseForSplit, valueForSplit);
        }

        // We create the new dictionary 
        ref var state = ref _internalCursor._stk[_internalCursor._pos];

        var page = _llt.AllocatePage(1);
        var header = (LookupPageHeader*)page.Pointer;
        header->PageFlags = state.Header->PageFlags;
        header->Lower = PageHeader.SizeOf;
        header->Upper = Constants.Storage.PageSize;
        header->FreeSpace = Constants.Storage.PageSize - PageHeader.SizeOf;

        if (header->PageFlags.HasFlag(LookupPageFlags.Branch))
        {
            _state.BranchPages++;
        }
        else
        {
            _state.LeafPages++;
        }

        // We need to ensure that we have the correct key before we change the page. 
        var splitKey = SplitPageEncodedEntries(ref currentCauseForSplit, page, valueForSplit, ref state);

        PopPage(ref _internalCursor); // add to parent

        SearchInCurrentPage(ref splitKey, ref _internalCursor._stk[_internalCursor._pos]);

        AddToPage(ref splitKey, page.PageNumber);
        
        if (_internalCursor._stk[_internalCursor._pos].Header->PageFlags == LookupPageFlags.Leaf)
        {
            // we change the structure of the tree, so we can't reuse the state
            // but we can only do that as the _last_ part of the operation, otherwise
            // recursive split page will give bad results
            InitializeCursorState(); 
        }

        VerifySizeOf(ref state, _internalCursor._pos == 0);
    }

    private TLookupKey SplitPageEncodedEntries(ref TLookupKey causeForSplit, Page page, long valueForSplit, ref CursorState state)
    {
        var newPageState = new CursorState { Page = page };
        var entryBufferPtr = stackalloc byte[EncodingBufferSize];
        newPageState.Header->KeysBase = causeForSplit.ToLong();
        newPageState.Header->ValuesBase = valueForSplit;

        // sequential write up, no need to actually split
        int numberOfEntries = state.Header->NumberOfEntries;
        if (numberOfEntries == state.LastSearchPosition && state.LastMatch > 0)
        {
            newPageState.LastSearchPosition = 0; // add as first

            var splitMarker = causeForSplit;
            
            if (state.Header->IsBranch) // we cannot allow a branch with a single element, steal another one
            {
                DecodeEntry(ref state, state.Header->NumberOfEntries - 1, out var k, out var v);
                RemoveFromPage(allowRecurse: false, state.Header->NumberOfEntries - 1, isExplicitRemove: false);
                // the first item in a branch is always the smallest
                var prevEntryLen = EncodeEntry(newPageState.Header, TLookupKey.MinValue, v, entryBufferPtr);
                AddEntryToPage(ref newPageState, prevEntryLen, entryBufferPtr, isUpdate: false);
         
                newPageState.LastSearchPosition++;
                
                splitMarker = TLookupKey.FromLong<TLookupKey>(k);
            }
            
            var entryLength = EncodeEntry(newPageState.Header, causeForSplit.ToLong(), valueForSplit, entryBufferPtr);
            AddEntryToPage(ref newPageState, entryLength, entryBufferPtr, isUpdate: false);

            return splitMarker;
        }

        // non sequential write, let's just split in middle
        int entriesCopied = 0;
        ushort* offsets = newPageState.EntriesOffsetsPtr;
        int i = FindPositionToSplitPageInHalfBasedOfEntriesSize(ref state, ref newPageState);

        for (; i < numberOfEntries; i++)
        {
            DecodeEntry(ref state, i, out var key, out var val);

            var entryLength = EncodeEntry(newPageState.Header, key, val, entryBufferPtr);

            newPageState.Header->Lower += sizeof(ushort);
            newPageState.Header->Upper -= (ushort)entryLength;
            newPageState.Header->FreeSpace -= (ushort)(entryLength + sizeof(ushort));
            offsets[entriesCopied++] = newPageState.Header->Upper;

            Debug.Assert(newPageState.Header->Lower <= newPageState.Header->Upper, "newPageState.Header->Lower <= newPageState.Header->Upper");
            Memory.Copy(page.Pointer + newPageState.Header->Upper, entryBufferPtr, entryLength);
        }
        state.Header->Lower -= (ushort)(sizeof(ushort) * entriesCopied);

        DefragPage(_llt, ref state); // need to ensure that we have enough space to add the new entry in the source page
        Debug.Assert(state.Header->FreeSpace <= Constants.Storage.PageSize - PageHeader.SizeOf,"state.Header->FreeSpace <= Constants.Storage.PageSize - PageHeader.SizeOf");
        Debug.Assert(state.Header->FreeSpace == state.ComputeFreeSpace(),"state.Header->FreeSpace == state.ComputeFreeSpace()");
        Debug.Assert(newPageState.Header->FreeSpace == newPageState.ComputeFreeSpace(),"newPageState.Header->FreeSpace == newPageState.ComputeFreeSpace()");

        ref CursorState updatedPageState = ref newPageState; // start with the new page
        int position = state.Header->NumberOfEntries - 1;
        var curKey = GetKeyData(ref state, position);
        if (causeForSplit.CompareTo(this, curKey) < 0)
        {
            // the new entry belong on the *old* page
            updatedPageState = ref state;
        }

        SearchInCurrentPage(ref causeForSplit, ref updatedPageState);
        Debug.Assert(updatedPageState.LastSearchPosition < 0, "There should be no updates here");
        updatedPageState.LastSearchPosition = ~updatedPageState.LastSearchPosition;
        {
            var entryLength = EncodeEntry(updatedPageState.Header, causeForSplit.ToLong(), valueForSplit, entryBufferPtr);
            Debug.Assert(updatedPageState.Header->Upper - updatedPageState.Header->Lower >= entryLength + sizeof(ushort));
            AddEntryToPage(ref updatedPageState, entryLength, entryBufferPtr, isUpdate: false);
        }

        long separatorKey = GetKeyData(ref newPageState, 0);
        if (newPageState.Header->IsBranch)
        {
            // here we insert a minimum value as the first item of the branch
            var k = TLookupKey.FromLong<TLookupKey>(separatorKey);
            // we need to update the separator key here...
            (separatorKey,_) = GetFirstActualKeyAndValue(newPageState.Header);
            RemoveEntryFromPage(ref newPageState, ref k, 0, isExplicitRemove: false, out var pageNum);
            var requiredSize = EncodeEntry(newPageState.Header, TLookupKey.MinValue, pageNum, entryBufferPtr);
            newPageState.LastSearchPosition = 0;
            AddEntryToPage(ref newPageState, requiredSize, entryBufferPtr, false);
        }

        VerifySizeOf(ref newPageState, isRoot: false);
        VerifySizeOf(ref state, isRoot: false);

        return TLookupKey.FromLong<TLookupKey>(separatorKey);
    }

    private static int FindPositionToSplitPageInHalfBasedOfEntriesSize(ref CursorState state, ref CursorState newPageState)
    {
        int sizeUsed = 0;
        var halfwaySizeMark = Constants.Storage.PageSize / 2;
        int numberOfEntries = state.Header->NumberOfEntries;
        var buffer = stackalloc byte[EncodingBufferSize];

        // here we have to guard against wildly unbalanced page structure, if the first 6 entries are 1KB each
        // and we have another 100 entries that are a byte each, if we split based on entry count alone, we'll 
        // end up unbalanced, so we compute the halfway mark based on the _size_ of the entries, not their count

        int i = numberOfEntries - 1;
        for (; i >= numberOfEntries/2; i--)
        {
            DecodeEntry(ref state, i, out var key, out var val);
            var len = EncodeEntry(newPageState.Header, key, val, buffer);
            var cost = len + sizeof(ushort);
            if (sizeUsed + cost > halfwaySizeMark)
                break;

            sizeUsed += cost;
        }
        return i;
    }

    [Conditional("DEBUG")]
    private static void VerifySizeOf(ref CursorState p, bool isRoot)
    {
        if (p.Header == null)
            return; // page may have been released

        if (p.Header->NumberOfEntries < 0)
        {
            throw new InvalidOperationException("Cannot have negative number of entries " + p.Page.PageNumber);
        }

        if (p.Header->IsBranch ^ p.Header->IsLeaf == false)
        {
            throw new InvalidOperationException("The page is corrupted, must be either leaf or branch " + p.Page.PageNumber);
        }
        if (p.Header->Upper - p.Header->Lower < 0)
        {
            throw new InvalidOperationException($"Lower {p.Header->Lower} > Upper {p.Header->Upper} " + p.Page.PageNumber);
        }

        if (p.Header->FreeSpace > Constants.Storage.PageSize - PageHeader.SizeOf)
        {
            throw new InvalidOperationException($"FreeSpace is too large: {p.Header->FreeSpace} " + p.Page.PageNumber);
        }
        
        if(p.Header->IsBranch && p.Header->NumberOfEntries <2)
            throw new InvalidOperationException($"Branch page with too few records: {p.Header->NumberOfEntries} " + p.Page.PageNumber);

        if (p.Header->IsLeaf && p.Header->NumberOfEntries == 0 && isRoot == false /* the root is allowed to be empty*/)
        {
            throw new InvalidOperationException($"Leaf page with too few records: {p.Header->NumberOfEntries} " + p.Page.PageNumber);
        }

        var actualFreeSpace = p.ComputeFreeSpace();
        if (p.Header->FreeSpace != actualFreeSpace)
        {
            throw new InvalidOperationException("The sizes do not match! FreeSpace: " + p.Header->FreeSpace + " but actually was space: " + actualFreeSpace  +", " + p.Page.PageNumber);
        }
    }

    private void CreateRootPage(ref TLookupKey k, long v)
    {
        _state.BranchPages++;

        ref var state = ref _internalCursor._stk[_internalCursor._pos];

        // we'll copy the current page and reuse it, to avoid changing the root page number
        var page = _llt.AllocatePage(1);

        long cpy = page.PageNumber;
        Debug.Assert(_llt.IsDirty(page.PageNumber));
        Memory.Copy(page.Pointer, state.Page.Pointer, Constants.Storage.PageSize);
        page.PageNumber = cpy;

        Debug.Assert(_llt.IsDirty(state.Page.PageNumber));
        Memory.Set(state.Page.DataPointer, 0, Constants.Storage.PageSize - PageHeader.SizeOf);
        state.Header->PageFlags = LookupPageFlags.Branch;
        state.Header->Lower = PageHeader.SizeOf + sizeof(ushort);
        state.Header->FreeSpace = Constants.Storage.PageSize - (PageHeader.SizeOf);
        state.Header->KeysBase = k.ToLong();
        state.Header->ValuesBase = v;


        var pageNumberBufferPtr = stackalloc byte[EncodingBufferSize];
        var size = EncodeEntry(state.Header, TLookupKey.MinValue, cpy, pageNumberBufferPtr);

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
            var tmpHeader = (LookupPageHeader*)tmpPtr;
            *tmpHeader = *(LookupPageHeader*)state.Page.Pointer;

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
            Debug.Assert(llt.IsDirty(state.Page.PageNumber));
            Memory.Copy(state.Page.Pointer, tmpPtr, Constants.Storage.PageSize);
            Debug.Assert(state.Header->FreeSpace == (state.Header->Upper - state.Header->Lower));
        }
    }

    public List<(TLookupKey, long)> AllEntriesIn(long p)
    {
        Page page = _llt.GetPage(p);
        var state = new CursorState { Page = page, };
        Debug.Assert(state.Header->IsBranch ^ state.Header->IsLeaf, "state.Header->IsBranch ^ state.Header->IsLeaf");

        var results = new List<(TLookupKey, long)>();

        using var scope = new CompactKeyCacheScope(this._llt);
        for (ushort i = 0; i < state.Header->NumberOfEntries; i++)
        {
            var key = GetKeyData(ref state, i);

            var val = GetValue(ref state, i);
            results.Add((TLookupKey.FromLong<TLookupKey>(key), val));
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
            Debug.Assert(state.Header->IsBranch ^ state.Header->IsLeaf, "state.Header->IsBranch ^ state.Header->IsLeaf");

            results.Add(p);
            if (state.Header->PageFlags.HasFlag(LookupPageFlags.Branch))
            {
                for (int i = 0; i < state.Header->NumberOfEntries; i++)
                {
                    var next = GetValue(ref state, i);
                    Add(next);
                }
            }
        }
    }
    private void FindPageFor(ref TLookupKey key, ref IteratorCursorState cstate)
    {
        cstate._pos = -1;
        cstate._len = 0;
        PushPage(_state.RootPage, ref cstate);

        ref var state = ref cstate._stk[cstate._pos];

        FindPageFor(ref cstate, ref state, ref key);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void FindPageFor(ref IteratorCursorState cstate, ref CursorState state, ref TLookupKey key)
    {
        while (state.Header->IsBranch)
        {
            SearchInCurrentPage(ref key, ref cstate._stk[cstate._pos]);

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
        SearchInCurrentPage(ref key, ref state);
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
        state.LastSearchPosition = 0;
        state.Page = _llt.GetPage(nextPage);
        Debug.Assert(state.Header->IsBranch ^ state.Header->IsLeaf, "state.Header->IsBranch ^ state.Header->IsLeaf");
        cstate._len++;
        VerifySizeOf(ref state, cstate._pos == 0);
    }

    private void SearchInCurrentPage(ref TLookupKey key, ref CursorState state)
    {
        SearchInCurrentPage(ref key, ref state, 0, state.Header->NumberOfEntries);
    }

    private void SearchInCurrentPage(ref TLookupKey key, ref CursorState state, int bot, int length)
    {
        Debug.Assert(state.Header->Upper - state.Header->Lower >= 0);
        Debug.Assert(state.Header->FreeSpace <= Constants.Storage.PageSize - PageHeader.SizeOf);

        ushort* @base = state.EntriesOffsetsPtr;
        byte* pagePtr = state.Page.Pointer;
        var header = state.Header;
        int match = -1;
        if (length == 0)
        {
            goto NotFound;
        }
        int top = length;
        long curKey;
        while (top > 1)
        {
            int mid = top / 2;

            curKey = GetKeyData(header, pagePtr + @base[bot + mid]);

            match = key.CompareTo(this, curKey);

            if (match >= 0)
                bot += mid;

            top -= mid;
        }

        curKey = GetKeyData(header, pagePtr + @base[bot]);

        match = key.CompareTo(this, curKey);

        if (match == 0)
        {
            state.LastMatch = 0;
            state.LastSearchPosition = bot;
            return;
        }

        NotFound:
        state.LastMatch = match > 0 ? 1 : -1;
        state.LastSearchPosition = ~(bot + (match > 0).ToInt32());
    }

    [Conditional("DEBUG")]
    public void Render(int steps = 1)
    {
        DebugStuff.RenderAndShow(this, steps);
    }

    /// <summary>
    /// Optimized to reduce the cost of traversing the tree
    /// Assumes that matches are sorted 
    /// </summary>
    public void GetFor(Span<long> keys, Span<long> terms, long missingValue)
    {
        if (typeof(TLookupKey) != typeof(Int64LookupKey))
        {
            throw new NotSupportedException(typeof(TLookupKey).FullName);
        }

        var lookupKey = TLookupKey.FromLong<TLookupKey>(keys[0]);
        FindPageFor(ref lookupKey, ref _internalCursor);
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        state.LastSearchPosition = 0;
        for (int i = 0; i < keys.Length; i++)
        {
            lookupKey = TLookupKey.FromLong<TLookupKey>(keys[i]);
            SearchInCurrentPage(ref lookupKey, ref state);
            if (state.LastMatch == 0) // found the value
            {
                terms[i] = GetValue(ref state,
                    // limit the search on the _next_ call on this page
                    state.LastSearchPosition);
                
                continue;
            }

            // didn't find the value, need to check if this is on this page by
            // checking if this is meant to go *after* the last value in the page
            if (~state.LastSearchPosition < state.Header->NumberOfEntries)
            {
                // it *should be* on this page, but isn't, we have a missing value
                terms[i] = missingValue;
                continue;
            }

            lookupKey = TLookupKey.FromLong<TLookupKey>(keys[i]);
            terms[i] = SearchNextPage(ref state, ref lookupKey);
        }

        long SearchNextPage(ref CursorState state, ref TLookupKey key)
        {
            while (_internalCursor._pos > 0)
            {
                PopPage(ref _internalCursor);
                state = ref _internalCursor._stk[_internalCursor._pos];
                var previousSearchPosition = state.LastSearchPosition;

                SearchInCurrentPage(ref key, ref state);

                if (state.LastSearchPosition < 0)
                    state.LastSearchPosition = ~state.LastSearchPosition;

                if (state.LastSearchPosition > previousSearchPosition &&
                    state.LastSearchPosition < state.Header->NumberOfEntries)
                {
                    // is this points to a different page, just search there normally
                    FindPageFor(ref _internalCursor, ref state, ref key);
                    state = ref _internalCursor._stk[_internalCursor._pos];
                    return state.LastMatch == 0 ? GetValue(ref state, state.LastSearchPosition) : missingValue;
                }
                // not here, need to go up
            }

            // got all the way to the top? Just search normally
            return TryGetValue(key, out var v) ? v : missingValue;
        }
    }

    public readonly struct PageRef
    {
        private readonly  Lookup<TLookupKey> _parent;

        public PageRef(Lookup<TLookupKey> parent)
        {
            _parent = parent;
        }
        
        public bool IsBranch => _parent._internalCursor._stk[_parent._internalCursor._pos].Header->IsBranch;
        public int NumberOfEntries => _parent._internalCursor._stk[_parent._internalCursor._pos].Header->NumberOfEntries;

        public TLookupKey GetKey(int i)
        {
            ref var state = ref _parent._internalCursor._stk[_parent._internalCursor._pos];
            var keyData= GetKeyData(ref state, i);
            return TLookupKey.FromLong<TLookupKey>(keyData);
        }
        
        public long GetValue(int i)
        {
            ref var state = ref _parent._internalCursor._stk[_parent._internalCursor._pos];
            return Lookup<TLookupKey>.GetValue(ref state, i);
        }

        public void PushPage(long p)
        {
            _parent.PushPage(p, ref _parent._internalCursor);
        }

        public bool GoToNextPage()
        {
            return _parent.GoToNextPage(ref _parent._internalCursor);
        }
    }

    public PageRef GetPageRef() => new(this);

    public void InitializeCursorState()
    {
        _internalCursor._pos = -1;
        _internalCursor._len = 0;
        PushPage(_state.RootPage, ref _internalCursor);
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        state.LastSearchPosition = 0;
    }

    /// <summary>
    /// The idea here is that we provide an optimization opportunity by reducing the total number of tree operations
    /// and allowing more efficient work afterward. This will find the first key in 'keys' and then fill the output
    /// values with all the _other_ keys that belong in the same page as the first key. Note that the key is whatever
    /// they *should* go there, so missing values are also included. 
    /// </summary>
    public int BulkUpdateStart(Span<TLookupKey> keys, Span<long> values, Span<int> offsets, out long pageNum)
    {
        Debug.Assert(keys.IsEmpty == false);
        // here we find the right page to start
        TryGetNextValue(ref keys[0], out values[0]);
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        offsets[0] = state.LastSearchPosition;
        pageNum = state.Page.PageNumber;

        var limit = CurrentPageLimit();
        int index = 1;
        for (; index < keys.Length; index++)
        {
            ref var curKey = ref keys[index];
            if (limit != null) // we have to use nullable because the value isn't directly comparable (term id, double,
            {
                if (curKey.CompareTo(this, limit.Value) >= 0)
                {
                    curKey.Reset(); // we don't want to retain any knowledge of this 
                    break; // hit the limits of the page...
                }
            }
            var pos = state.LastSearchPosition;
            if (pos < 0)
                pos = ~pos;
            if (pos == state.Header->NumberOfEntries)
            {
                // nothing more on this page, we'll continue until we consume all the keys
                // or we hit the limit for this page 
                values[index] = -1;
                offsets[index] = ~state.Header->NumberOfEntries;
                continue;
            } 

            var (start, end) = ExponentialSearchPlacement(ref curKey, ref state, pos);
            SearchInCurrentPage(ref curKey, ref state, start, end - start);
            values[index] = state.LastSearchPosition < 0 ? -1 : GetValue(ref state, state.LastSearchPosition);
            offsets[index] = state.LastSearchPosition;
        }

        return index;

        (int Start, int End) ExponentialSearchPlacement(ref TLookupKey curKey, ref CursorState state, int last)
        {
            last = Math.Max(1, last);
            while (last < state.Header->NumberOfEntries)
            {
                long keyData = GetKeyData(ref state, last);
                if (curKey.CompareTo(this, keyData) < 0)
                {
                    break;
                }

                last *= 2;
            }

            return (last / 2, Math.Min(last + 1, state.Header->NumberOfEntries));
        }
    }

    public TreeStructureChanged CheckTreeStructureChanges() => new TreeStructureChanged(this);

    public readonly struct TreeStructureChanged
    {
        private readonly Lookup<TLookupKey> _parent;
        private readonly int _initialVersion;

        public TreeStructureChanged(Lookup<TLookupKey> parent)
        {
            _parent = parent;
            _initialVersion = _parent._treeStructureVersion;
        }

        public bool Changed => _initialVersion != _parent._treeStructureVersion;
    }

    /// <summary>
    /// Other side of BulkUpdateStart. Will accept keys that were used by BulkInsertStart and insert them
    /// into the _current_ page in the relevant offset.
    /// Can handle adding new items as well as updating existing ones
    /// if the structure of the tree changed during the operation, it ***invalidates*** the previous
    /// BulkUpdateStart results.
    /// </summary>
    public void BulkUpdateSet(ref TLookupKey key, long value, long pageNum, int offset, ref int adjustment)
    {
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        if (state.Page.PageNumber != pageNum)
            throw new InvalidOperationException("Different page number provided from BulkUpdateSet");

        if (offset >= 0)
        {
            state.LastSearchPosition = offset + adjustment;
        }
        else
        {
            state.LastSearchPosition = offset - adjustment;
            adjustment++; // increment all future actions by one to account for the new item
        }

        // need to go negative because the value is negative and then flipped

        AddToPage(ref key, value, searchForKeyOnSplit: true);
    }

    /// <summary>
    /// Other side of BulkUpdateStart. Will accept keys that were used by BulkInsertStart and remove them
    /// from the _current_ page in the relevant offset.
    /// if the structure of the tree changed during the operation, it ***invalidates*** the previous
    /// BulkUpdateStart results.
    /// </summary>
    public bool BulkUpdateRemove(ref TLookupKey key, long pageNum, int offset, ref int adjustment, out long oldValue)
    {
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        if (state.Page.PageNumber != pageNum)
            throw new InvalidOperationException("Different page number provided from BulkUpdateSet");

        if (offset < 0)
        {
            oldValue = -1;
            return false; // want to removed a missing value, nothing to do
        }
    

        state.LastSearchPosition = offset + adjustment;
        adjustment--; // all future operations are now *reduced*, to account for the removal
        
        // need to go negative because the value is negative and then flipped
        state.LastMatch = 0; // ensure that we mark it properly
        oldValue = GetValue(ref state, state.LastSearchPosition);
        RemoveFromPage(ref key, allowRecurse: true, isExplicitRemove: true);
        return true;
    }
    
    
    private long? CurrentPageLimit()
    {
        for (int i = _internalCursor._pos - 1; i >= 0; i--)
        {
            ref var cur = ref _internalCursor._stk[i];
            if (cur.LastSearchPosition + 1 >= cur.Header->NumberOfEntries)
                continue;

            return GetKeyData(ref cur, cur.LastSearchPosition + 1);
        }

        return null;
    }
    
    
    public bool TryGetNextValue(ref TLookupKey key, out long value)
    {
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        if (state.Header->PageFlags == LookupPageFlags.Branch)
        {
            FindPageFor(ref _internalCursor, ref state, ref key);
            state = ref _internalCursor._stk[_internalCursor._pos];

            if (state.LastMatch == 0) // found it
            {
                value = GetValue(ref state, state.LastSearchPosition);
                return true;
            }
            // did *not* find it, but we are somewhere on the tree that is ensured
            // to be at the key location *or before it*, so we can now start scanning *up*
        }

        Debug.Assert(state.Header->PageFlags == LookupPageFlags.Leaf, $"Got {state.Header->PageFlags} flag instead of {nameof(LookupPageFlags.Leaf)}");

        SearchInCurrentPage(ref key, ref state);
        if (state.LastSearchPosition >= 0) // found it, yeah!
        {
            value = GetValue(ref state, state.LastSearchPosition);
            return true;
        }

        if (KeyShouldBeInCurrentPage(ref key, ref state))
        {
            // we didn't find the key, but we found a _greater_ key in the page
            // therefore, we don't have it (we know the previous key was in this page
            // so if there is a greater key in this page, we didn't find it
            value = -1;
            return false;
        }

        while (_internalCursor._pos > 0)
        {
            PopPage(ref _internalCursor);
            state = ref _internalCursor._stk[_internalCursor._pos];
            var previousSearchPosition = state.LastSearchPosition;

            SearchInCurrentPage(ref key, ref state);

            if (state.LastSearchPosition < 0)
                state.LastSearchPosition = ~state.LastSearchPosition;

            // is this points to a different page, just search there normally
            if (state.LastSearchPosition > previousSearchPosition && state.LastSearchPosition < state.Header->NumberOfEntries)
            {
                FindPageFor(ref _internalCursor, ref state, ref key);
                state = ref _internalCursor._stk[_internalCursor._pos];
                if (state.LastMatch != 0)
                {
                    value = -1;
                    return false;
                }

                value = GetValue(ref state, state.LastSearchPosition);
                return true;
            }
        }

        // if we go to here, we are at the root, so operate normally
        return TryGetValue(ref key, out value);
    }

    private bool KeyShouldBeInCurrentPage(ref TLookupKey key, ref CursorState state)
    {
        var pos = ~state.LastSearchPosition;
        var shouldBeInCurrentPage = pos < state.Header->NumberOfEntries;
        if (shouldBeInCurrentPage)
        {
            var curKey = GetKeyData(ref state, pos);
            var match = key.CompareTo(this, curKey);

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

                var curKey = GetKeyData(ref cur, cur.LastSearchPosition + 1);
                var match = key.CompareTo(this, curKey);
                if (match < 0)
                    continue;

                shouldBeInCurrentPage = false;
                break;
            }
        }

        return shouldBeInCurrentPage;
    }

    public void VerifyStructure()
    {
        var state = new CursorState { Page = _llt.GetPage(_state.RootPage) };
        if (state.Header->NumberOfEntries == 0)
            return;
        VerifySizeOf(ref state, true);
        var previous = TLookupKey.FromLong<TLookupKey>(TLookupKey.MinValue);
        previous.Init(this);
        VerifyStructure(state, ref previous);

    }

    private void VerifyStructure(CursorState state, ref TLookupKey previous)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        for (int i = 0; i < state.Header->NumberOfEntries; i++)
        {
            GetKeyAndValue(ref state, i,out var keyData, out var value);
            var key = TLookupKey.FromLong<TLookupKey>(keyData);
            key.Init(this);
            if (i == 0 && state.Header->IsBranch)
            {
                if (keyData != TLookupKey.MinValue)
                    throw new InvalidOperationException("First item on branch page isn't the minimum value!");
            }
            else
            {
                if (previous.CompareTo(key) > 0)
                    throw new InvalidOperationException("Unsorted values: " + previous + " >= " + key + " on " + state.Header->PageNumber);
            }
            previous = key;

            if (state.Header->IsBranch)
            {
                // we recurse down and *update* the previous, so we get cross page verification as well
                var child = new CursorState { Page = _llt.GetPage(value) };
                VerifySizeOf(ref child, false);
                VerifyStructure(child, ref previous);
            }
        }
    }
}
