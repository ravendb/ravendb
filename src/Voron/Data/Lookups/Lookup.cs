using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
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
        var  buffer = state.Page.Pointer + state.EntriesOffsetsPtr[pos];
        return DecodeEntry(state.Header, buffer, out keyData, out val);
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
        buffer = state.Page.Pointer + state.EntriesOffsetsPtr[pos];
        var keyLen = buffer[0] >> 4;
        var valLen = buffer[0] & 0xF;
        return keyLen + valLen + 1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetEntrySize(ref CursorState state, int pos)
    {
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
    private static long GetKeyData(ref CursorState state, int pos)
    {
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
        var buffer = state.Page.Pointer + state.EntriesOffsetsPtr[pos];
        var keyLen = buffer[0] >> 4;
        var valLen = buffer[0] & 0xF;
        long v = ReadBackward(buffer + 1 + keyLen, valLen);

        return Unzag(v, state.Header->ValuesBase);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void GetKeyAndValue(ref CursorState state, int pos, out long key, out long value)
    {
        var buffer = state.Page.Pointer + state.EntriesOffsetsPtr[pos];
        var keyLen = buffer[0] >> 4;
        var valLen = buffer[0] & 0xF;
        long k = ReadBackward(buffer + 1, keyLen);
        long v = ReadBackward(buffer + 1 + keyLen, valLen);
        key = Unzag(k, state.Header->KeysBase);
        value = Unzag(v, state.Header->ValuesBase);
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

        LookupState* header;

        var existing = parent.Read(name);
        if (existing == null)
        {
            if (llt.Flags != TransactionFlags.ReadWrite)
                return null;


            var newPage = llt.AllocatePage(1);
            var compactPageHeader = (LookupPageHeader*)newPage.Pointer;
            compactPageHeader->PageFlags = LookupPageFlags.Leaf;
            compactPageHeader->Lower = PageHeader.SizeOf;
            compactPageHeader->Upper = Constants.Storage.PageSize;
            compactPageHeader->FreeSpace = Constants.Storage.PageSize - (PageHeader.SizeOf);
            compactPageHeader->KeysBase = newPage.PageNumber;
            compactPageHeader->ValuesBase = newPage.PageNumber;

            using var _ = parent.DirectAdd(name, sizeof(LookupState), out var p);
            header = (LookupState*)p;
            *header = new LookupState
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
        else
        {
            header = (LookupState*)existing.Reader.Base;
        }

        if (header->RootObjectType != RootObjectType.Lookup)
            throw new InvalidOperationException($"Tried to open {name} as a lookup, but it is actually a " +
                                                header->RootObjectType);

        return new Lookup<TLookupKey>(name, parent)
        {
            _llt = llt,
            _state = *header
        };
    }

    public void PrepareForCommit()
    {
        using var _ = _parent.DirectAdd(Name, sizeof(LookupState), out var ptr);
        _state.CopyTo((LookupState*)ptr);
    }

    public bool TryGetValue(TLookupKey key, out long value) => TryGetValue(ref key, out value);
    public bool TryGetValue(ref TLookupKey key, out long value)
    {
        FindPageFor(ref key, ref _internalCursor);
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        if (state.LastMatch != 0)
        {
            value = default;
            return false;
        }

        value = GetValue(ref state, state.LastSearchPosition);
        return true;
    }

    public bool TryRemove(TLookupKey key) => TryRemove(ref key);

    public bool TryRemove(ref TLookupKey key)
    {
        FindPageFor(ref key, ref _internalCursor);
        return RemoveFromPage(ref key, allowRecurse: true);
    }

    public bool TryRemove(TLookupKey key, out long value) => TryRemove(ref key, out value);
    
    public bool TryRemove(ref TLookupKey key, out long value)
    {
        FindPageFor(ref key, ref _internalCursor);
        if (_internalCursor._stk[_internalCursor._pos].LastMatch != 0)
        {
            value = default;
            return false;
        }
        return TryRemoveExistingValue(ref key, out value);
    }

    public bool TryRemoveExistingValue(ref TLookupKey key, out long value)
    {
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        value = GetValue(ref state, state.LastSearchPosition);
        return RemoveFromPage(ref key, allowRecurse: true);
    }

    private void RemoveFromPage(bool allowRecurse, int pos)
    {
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        state.LastSearchPosition = pos;
        state.LastMatch = 0;
        long keyData = GetKeyData(ref state, pos);
        var key = TLookupKey.FromLong<TLookupKey>(keyData);
        RemoveFromPage(ref key, allowRecurse);
    }

    private bool RemoveFromPage(ref TLookupKey key, bool allowRecurse)
    {
        ref var state = ref _internalCursor._stk[_internalCursor._pos];
        if (state.LastMatch != 0)
        {
            return false;
        }
        state.Page = _llt.ModifyPage(state.Page.PageNumber);

        var entriesOffsets = state.EntriesOffsets;
        var len = GetEntryBuffer(ref state, state.LastSearchPosition, out _);

        state.Header->FreeSpace += (ushort)(sizeof(ushort) + len);
        state.Header->Lower -= sizeof(short); // the upper will be fixed on defrag

        Debug.Assert(state.Header->Upper - state.Header->Lower >= 0);
        Debug.Assert(state.Header->FreeSpace <= Constants.Storage.PageSize - PageHeader.SizeOf);

        key.OnKeyRemoval(this);
        
        entriesOffsets[(state.LastSearchPosition + 1)..].CopyTo(entriesOffsets[state.LastSearchPosition..]);

        if (state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf))
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
                InitializeCursorState(); // we change the structure of the tree, so we can't reuse 
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
            }
            return true;
        }

        var siblingPage = GetValue(ref parent, parent.LastSearchPosition + 1);
        sourceState = new CursorState
        {
            Page = _llt.ModifyPage(siblingPage)
        };

        if (sourceState.Header->PageFlags != destinationState.Header->PageFlags)
            return false; // cannot merge leaf & branch pages

        var destinationPage = destinationState.Page;
        var destinationHeader = destinationState.Header;
        var sourcePage = sourceState.Page;
        var sourceHeader = sourceState.Header;

        // the new entries size is composed of entries from _both_ pages            
        var entries = new Span<ushort>(destinationPage.Pointer + PageHeader.SizeOf, destinationHeader->NumberOfEntries + sourceHeader->NumberOfEntries)
                            .Slice(destinationHeader->NumberOfEntries);

        bool reEncode = sourceHeader->KeysBase != destinationHeader->KeysBase ||
                        sourceHeader->ValuesBase != destinationHeader->ValuesBase;

        int sourceMovedLength = 0;
        int sourceKeysCopied = 0;
        {
            for (; sourceKeysCopied < sourceHeader->NumberOfEntries; sourceKeysCopied++)
            {
                var copied = reEncode
                    ? MoveEntryWithReEncoding(ref destinationState, entries)
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


        PopPage(ref _internalCursor);

        // we aren't _really_ removing, so preventing merging of parents
        RemoveFromPage(allowRecurse: false, parent.LastSearchPosition + 1);

        // Ensure that we got the right key to search. 
        var newKey = TLookupKey.FromLong<TLookupKey>(GetKeyData(ref sourceState, 0));

        SearchInCurrentPage(ref newKey, ref _internalCursor._stk[_internalCursor._pos]); // positions changed, re-search
        AddToPage(ref newKey, siblingPage);
        return true;
        
        [SkipLocalsInit]
        bool MoveEntryWithReEncoding(ref CursorState destinationState, Span<ushort> entries)
        {
            // PERF: This method is marked SkipLocalInit because we want to avoid initialize these values
            // as we are going to be writing them anyways.
            byte* entryBuffer = stackalloc byte[EncodingBufferSize];
            //We get the encoded key and value from the sibling page
            var originalEntrySize = DecodeEntry(ref sourceState, sourceKeysCopied, out var key, out var value);

            // If we don't have enough free space in the receiving page, we move on. 
            var requiredSize = EncodeEntry(destinationState.Header, key, value, entryBuffer);
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
    
    public void Add(TLookupKey key, long value)
    {
        Add(ref key, value);
    }

    [SkipLocalsInit]
    private void AddToPage(ref TLookupKey key, long value)
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

            // remove the entry, we'll need to add it as new
            int entriesCount = state.Header->NumberOfEntries;
            ushort* stateEntriesOffsetsPtr = state.EntriesOffsetsPtr;
            for (int i = state.LastSearchPosition; i < entriesCount - 1; i++)
            {
                stateEntriesOffsetsPtr[i] = stateEntriesOffsetsPtr[i + 1];
            }

            state.Header->Lower -= sizeof(short);
            state.Header->FreeSpace += (ushort)(sizeof(short) + len);
            if (state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf))
                _state.NumberOfEntries--; // we aren't counting branch entries
        }

        Debug.Assert(state.Header->FreeSpace >= (state.Header->Upper - state.Header->Lower));
        if (state.Header->Upper - state.Header->Lower < requiredSize + sizeof(short))
        {
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
                SplitPage(ref key, value);
                // a page split may cause us to do a search and reset the existing container id
                //DebugStuff.RenderAndShow(this);
                return;
            }
        }

        AddEntryToPage(ref state, requiredSize, entryBufferPtr);
        VerifySizeOf(ref state);
    }

    private void AddEntryToPage(ref CursorState state, int requiredSize, byte* entryBufferPtr)
    {
        //VerifySizeOf(ref state);

        state.Header->Lower += sizeof(short);
        var newEntriesOffsets = state.EntriesOffsets;
        var newNumberOfEntries = state.Header->NumberOfEntries;

        ushort* newEntriesOffsetsPtr = state.EntriesOffsetsPtr;
        for (int i = newNumberOfEntries - 1; i >= state.LastSearchPosition; i--)
            newEntriesOffsetsPtr[i] = newEntriesOffsetsPtr[i - 1];

        if (state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf))
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

    private void SplitPage(ref TLookupKey currentCauseForSplit, long valueForSplit)
    {
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

        VerifySizeOf(ref state);
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
            var entryLength = EncodeEntry(newPageState.Header, causeForSplit.ToLong(), valueForSplit, entryBufferPtr);
            AddEntryToPage(ref newPageState, entryLength, entryBufferPtr);
            return causeForSplit;
        }

        // non sequential write, let's just split in middle
        int entriesCopied = 0;
        int sizeCopied = 0;
        ushort* offsets = newPageState.EntriesOffsetsPtr;
        int i = FindPositionToSplitPageInHalfBasedOfEntriesSize(ref state, ref newPageState);

        for (; i < numberOfEntries; i++)
        {
            DecodeEntry(ref state, i, out var key, out var val);

            var entryLength = EncodeEntry(newPageState.Header, key, val, entryBufferPtr);

            newPageState.Header->Lower += sizeof(ushort);
            newPageState.Header->Upper -= (ushort)entryLength;
            newPageState.Header->FreeSpace -= (ushort)(entryLength + sizeof(ushort));
            sizeCopied += entryLength + sizeof(ushort);

            Debug.Assert(sizeCopied <= Constants.Storage.PageSize - PageHeader.SizeOf);

            offsets[entriesCopied++] = newPageState.Header->Upper;
            Memory.Copy(page.Pointer + newPageState.Header->Upper, entryBufferPtr, entryLength);
        }
        state.Header->Lower -= (ushort)(sizeof(ushort) * entriesCopied);
        state.Header->FreeSpace += (ushort)(sizeCopied);
        Debug.Assert(state.Header->FreeSpace <= Constants.Storage.PageSize - PageHeader.SizeOf);

        DefragPage(_llt, ref state); // need to ensure that we have enough space to add the new entry in the source page

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
            AddEntryToPage(ref updatedPageState, entryLength, entryBufferPtr);
        }
        VerifySizeOf(ref newPageState);
        VerifySizeOf(ref state);

        return TLookupKey.FromLong<TLookupKey>(GetKeyData(ref newPageState, 0));
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

    private void CreateRootPage(ref TLookupKey k, long v)
    {
        _state.BranchPages++;

        ref var state = ref _internalCursor._stk[_internalCursor._pos];

        // we'll copy the current page and reuse it, to avoid changing the root page number
        var page = _llt.AllocatePage(1);

        long cpy = page.PageNumber;
        Memory.Copy(page.Pointer, state.Page.Pointer, Constants.Storage.PageSize);
        page.PageNumber = cpy;

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
            Unsafe.CopyBlockUnaligned(state.Page.Pointer, tmpPtr, Constants.Storage.PageSize);
            Debug.Assert(state.Header->FreeSpace == (state.Header->Upper - state.Header->Lower));
        }
    }

    public List<(TLookupKey, long)> AllEntriesIn(long p)
    {
        Page page = _llt.GetPage(p);
        var state = new CursorState { Page = page, };

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
        state.Page = _llt.GetPage(nextPage);
        cstate._len++;
    }

    private void SearchInCurrentPage(ref TLookupKey key, ref CursorState state, int bot = 0)
    {
        Debug.Assert(state.Header->Upper - state.Header->Lower >= 0);
        Debug.Assert(state.Header->FreeSpace <= Constants.Storage.PageSize - PageHeader.SizeOf);

        ushort* @base = state.EntriesOffsetsPtr;
        byte* pagePtr = state.Page.Pointer;
        var header = state.Header;
        int length = state.Header->NumberOfEntries;
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
    public void Render()
    {
        DebugStuff.RenderAndShow(this);
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

    public bool TryGetNextValue(TLookupKey key, out long value) => TryGetNextValue(ref key, out value);
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

                var curKey = GetKeyData(ref cur, cur.LastSearchPosition+1 );
                var match = key.CompareTo(this, curKey);
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
                    value = default;
                    return false;
                }

                value = GetValue(ref state, state.LastSearchPosition);
                return true;
            }
        }

        // if we go to here, we are at the root, so operate normally
        return TryGetValue(ref key, out value);
    }
}
