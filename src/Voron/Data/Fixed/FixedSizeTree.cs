// -----------------------------------------------------------------------
//  <copyright file="FixedSizeTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Server;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.Paging;
using Constants = Voron.Global.Constants;

namespace Voron.Data.Fixed
{
    public unsafe partial class FixedSizeTree : IDisposable
    {
#if VALIDATE_DIRECT_ADD_STACKTRACE
        private string _allocationStacktrace;
#endif

        internal const int BranchEntrySize = sizeof(long) + sizeof(long);
        private readonly LowLevelTransaction _tx;
        private readonly Tree _parent;
        private Slice _treeName;
        private readonly ushort _valSize;
        private readonly bool _isIndexTree;
        private readonly int _entrySize;
        private readonly int _maxEmbeddedEntries;

        private NewPageAllocator _newPageAllocator;
        private FastStack<FixedSizeTreePage> _cursor;
        private int _changes;

        public LowLevelTransaction Llt => _tx;

        internal RootObjectType? Type
        {
            get
            {
                var header = _parent.DirectRead(_treeName);
                if (header == null)
                    return null;

                return ((FixedSizeTreeHeader.Embedded*)header)->RootObjectType;
            }
        }

        public bool HasNewPageAllocator => _newPageAllocator != null;

        public static ushort GetValueSize(LowLevelTransaction tx, Tree parent, Slice treeName)
        {
            var header = (FixedSizeTreeHeader.Embedded*)parent.DirectRead(treeName);
            if (header == null)
                throw new InvalidOperationException("No such tree: " + treeName);

            switch (header->RootObjectType)
            {
                case RootObjectType.EmbeddedFixedSizeTree:
                case RootObjectType.FixedSizeTree:
                    break;
                default:
                    throw new ArgumentOutOfRangeException("Tried to open '" + treeName + "' as FixedSizeTree, but is actually " + header->RootObjectType);
            }

            return header->ValueSize;
        }

        public struct DirectAddScope : IDisposable
        {
            private readonly FixedSizeTree _parent;

            public DirectAddScope(FixedSizeTree parent)
            {
                _parent = parent;
                if (_parent._directAddUsage++ != 0)
                {
                    ThrowScopeAlreadyOpen();
                }

#if VALIDATE_DIRECT_ADD_STACKTRACE
                _parent._allocationStacktrace = Environment.StackTrace;
#endif
            }

            public void Dispose()
            {
                _parent._directAddUsage--;
            }


            private void ThrowScopeAlreadyOpen()
            {
                var message = $"Write operation already requested on a tree name: {_parent}. " +
                              $"{nameof(Tree.DirectAdd)} method cannot be called recursively while the scope is already opened.";

#if VALIDATE_DIRECT_ADD_STACKTRACE
                message += Environment.NewLine + _parent._allocationStacktrace;
#endif

                throw new InvalidOperationException(message);
            }

        }

        public void RepurposeInstance(Slice treeName, bool clone)
        {
            new DirectAddScope(this).Dispose();// verifying that we aren't holding a ptr out

            if (clone)
            {
                if (_treeName.HasValue)
                    _tx.Allocator.Release(ref _treeName.Content);

                _treeName = treeName.Clone(_tx.Allocator);
            }
            else
            {
                _treeName = treeName;
            }

            var header = (FixedSizeTreeHeader.Embedded*)_parent.DirectRead(_treeName);
            if (header == null)
                return;

            switch (header->RootObjectType)
            {
                case RootObjectType.EmbeddedFixedSizeTree:
                case RootObjectType.FixedSizeTree:
                    break;
                default:
                    ThrowInvalidFixedSizeTree(treeName, header);
                    break; // will never get here
            }

            if (header->ValueSize != _valSize)
                ThrowInvalidFixedSizeTreeSize(header);
        }

        private void ThrowInvalidFixedSizeTreeSize(FixedSizeTreeHeader.Embedded* header)
        {
            throw new InvalidFixedSizeTree("The expected value len " + _valSize + " does not match actual value len " +
                                                header->ValueSize + " for " + _treeName);
        }

        private static void ThrowInvalidFixedSizeTree(Slice treeName, FixedSizeTreeHeader.Embedded* header)
        {
            throw new InvalidFixedSizeTree("Tried to open '" + treeName + "' as FixedSizeTree, but is actually " +
                                                  header->RootObjectType);
        }

        public FixedSizeTree(LowLevelTransaction tx, Tree parent, Slice treeName, ushort valSize, bool clone = true, bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            _tx = tx;
            _parent = parent;
            _valSize = valSize;
            _isIndexTree = isIndexTree;

            if (newPageAllocator != null)
                SetNewPageAllocator(newPageAllocator);

            _entrySize = sizeof(long) + _valSize;
            _maxEmbeddedEntries = (Constants.Storage.PageSize / 8) / _entrySize;
            if (_maxEmbeddedEntries == 0)
                ThrowInvalidFixedTreeValueSize();

            RepurposeInstance(treeName, clone);
        }

        private static void ThrowInvalidFixedTreeValueSize()
        {
            throw new InvalidFixedSizeTree("The value size must be small than " + (Constants.Storage.PageSize / 8));
        }

        public long[] Debug(FixedSizeTreePage p)
        {
            var entrySize = _entrySize;
            return Debug(p, entrySize);
        }

        public static long[] Debug(FixedSizeTreePage p, int entrySize)
        {
            if (p == null)
                return null;
            return Debug(p.Pointer + p.StartPosition, p.NumberOfEntries,
                p.IsLeaf ? entrySize : BranchEntrySize);
        }

        public Slice Name => _treeName;

        public static long[] Debug(byte* p, int entries, int size)
        {
            var a = new long[entries];
            for (int i = 0; i < entries; i++)
            {
                a[i] = *((long*)(p + (size * i)));
            }
            return a;
        }

        public bool Add(long key)
        {
            return Add(key, default(Slice));
        }

        public bool Add(long key, Slice val)
        {
            if (_valSize == 0 && val.HasValue && val.Size != 0)
                throw new InvalidOperationException("When the value size is zero, no value can be specified");
            if (_valSize != 0 && !val.HasValue)
                throw new InvalidOperationException("When the value size is not zero, the value must be specified");
            if (val.HasValue && val.Size != _valSize)
                throw new InvalidOperationException($"The value size must be of size '{_valSize}' but was of size '{val.Size}'.");

            bool isNew;
            byte* ptr;
            using (DirectAdd(key, out isNew, out ptr))
            {
                if (val.HasValue && val.Size != 0)
                    val.CopyTo(ptr);
            }

            return isNew;
        }

        public bool Add(long key, byte[] val)
        {
            Slice str;
            using (Slice.From(_tx.Allocator, val, ByteStringType.Immutable, out str))
            {
                return Add(key, str);
            }
        }

        public DirectAddScope DirectAdd(long key, out bool isNew, out byte* ptr)
        {
            if (_tx.Flags == TransactionFlags.Read)
                throw new InvalidOperationException("Cannot add a value in a read only transaction");

            _changes++;
            byte* pos;
            switch (Type)
            {
                case null:
                    pos = AddNewEntry(key);
                    isNew = true;
                    break;
                case RootObjectType.EmbeddedFixedSizeTree:
                    pos = AddEmbeddedEntry(key, out isNew);
                    break;
                case RootObjectType.FixedSizeTree:
                    pos = AddLargeEntry(key, out isNew);
                    break;
                default:
                    ThrowInvalidFixedSizeTreeType();
                    pos = null; // never happens
                    isNew = false;
                    break;
            }
            ptr = pos;
            return new DirectAddScope(this);
        }

        private void ThrowInvalidFixedSizeTreeType()
        {
            throw new InvalidFixedSizeTree(Type?.ToString());
        }

        private byte* AddLargeEntry(long key, out bool isNew)
        {
            var page = FindPageFor(key);

            page = ModifyPage(page);

            if (_lastMatch == 0) // update
            {
                isNew = false;
                return page.Pointer + page.StartPosition + (page.LastSearchPosition * _entrySize) + sizeof(long);
            }


            if (page.LastMatch > 0)
                page.LastSearchPosition++; // after the last one

            if ((page.NumberOfEntries + 1) * _entrySize > page.PageMaxSpace)
            {
                PageSplit(page, key);

                // now we know we have enough space, or we need to split the parent pageNum
                var addLargeEntry = AddLargeEntry(key, out isNew);
                isNew = true;
                ValidateTree();
                return addLargeEntry;
            }

            FixedSizeTreeHeader.Large* header;
            using (ModifyLargeHeader(out header))
            {
                page.ResetStartPosition();

                var entriesToMove = page.NumberOfEntries - page.LastSearchPosition;
                if (entriesToMove > 0)
                {
                    Memory.Move(page.Pointer + page.StartPosition + ((page.LastSearchPosition + 1) * _entrySize),
                        page.Pointer + page.StartPosition + (page.LastSearchPosition * _entrySize),
                        entriesToMove * _entrySize);
                }

                page.NumberOfEntries++;
                header->NumberOfEntries++;

                isNew = true;
                *((long*)(page.Pointer + page.StartPosition + (page.LastSearchPosition * _entrySize))) = key;

                ValidateTree();

                return (page.Pointer + page.StartPosition + (page.LastSearchPosition * _entrySize) + sizeof(long));
            }
        }

        [Conditional("VALIDATE")]
        private void ValidateTree()
        {
            ValidateTree_Forced();
        }

        public void ValidateTree_Forced()
        {
            if (Type != RootObjectType.FixedSizeTree)
                return;

            var header = (FixedSizeTreeHeader.Large*)_parent.DirectRead(_treeName);

            var stack = new Stack<FixedSizeTreePage>();
            stack.Push(GetReadOnlyPage(header->RootPageNumber));

            var numberOfEntriesInTree = 0;

            while (stack.Count > 0)
            {
                var cur = stack.Pop();

                if (cur.NumberOfEntries == 0)
                    throw new InvalidOperationException($"Page {cur.PageNumber} has no entries");

                var prev = cur.GetKey(0);
                if (cur.IsBranch)
                    stack.Push(GetReadOnlyPage(cur.GetEntry(0)->PageNumber));
                else
                    numberOfEntriesInTree++;

                for (int i = 1; i < cur.NumberOfEntries; i++)
                {
                    var curKey = cur.GetKey(i);
                    if (prev >= curKey)
                        throw new InvalidOperationException($"Page {cur.PageNumber} is not sorted");

                    if (cur.IsBranch)
                        stack.Push(GetReadOnlyPage(cur.GetEntry(i)->PageNumber));
                    else
                        numberOfEntriesInTree++;
                }
            }

            if (numberOfEntriesInTree != header->NumberOfEntries)
            {
                throw new InvalidOperationException($"Expected number of entries {header->NumberOfEntries}, actual {numberOfEntriesInTree}");
            }
        }

        internal FixedSizeTreePage GetReadOnlyPage(long pageNumber)
        {
            var readOnlyPage = _tx.GetPage(pageNumber);
            return new FixedSizeTreePage(readOnlyPage.Pointer, _entrySize, Constants.Storage.PageSize);
        }

        private FixedSizeTreePage FindPageFor(long key)
        {
            var header = (FixedSizeTreeHeader.Large*)_parent.DirectRead(_treeName);
            var page = GetReadOnlyPage(header->RootPageNumber);
            if (_cursor == null)
                _cursor = new FastStack<FixedSizeTreePage>();
            else
                _cursor.WeakClear();

            while (page.IsLeaf == false)
            {
                _cursor.Push(page);
                BinarySearch(page, key);
                if (page.LastMatch < 0 && page.LastSearchPosition > 0)
                    page.LastSearchPosition--;
                var childPageNumber = page.GetEntry(page.LastSearchPosition)->PageNumber;
                page = GetReadOnlyPage(childPageNumber);
            }

            BinarySearch(page, key);
            return page;
        }

        private FixedSizeTreePage NewPage(FixedSizeTreePageFlags flags, long nearbyPage)
        {
            FixedSizeTreePage allocatePage;

            using (FreeSpaceTree ? _tx.Environment.FreeSpaceHandling.Disable() : null)
            {
                // we cannot recursively call free space handling to ensure that we won't modify a section
                // relevant for a page which is currently being changed allocated

                var page = _newPageAllocator?.AllocateSinglePage(nearbyPage) ?? _tx.AllocatePage(1);
                allocatePage = new FixedSizeTreePage(page.Pointer, _entrySize, Constants.Storage.PageSize);
            }

            allocatePage.Dirty = true;
            allocatePage.FixedTreeFlags = flags;
            allocatePage.Flags = PageFlags.Single | PageFlags.FixedSizeTreePage;
            return allocatePage;
        }

        private void FreePage(long pageNumber, bool modifyPageCount = true)
        {
            if (modifyPageCount)
            {
                using (ModifyLargeHeader(out var largeHeader))
                    largeHeader->PageCount--;
            }

            if (FreeSpaceTree)
            {
                // we cannot recursively call free space handling to ensure that we won't modify a section
                // relevant for a page which is currently being freed, so we will free it on tx commit

                _tx.FreePageOnCommit(pageNumber);
            }
            else
            {

                if (_newPageAllocator != null)
                {
                    if (_isIndexTree == false)
                        Tree.ThrowAttemptToFreePageToNewPageAllocator(Name, pageNumber);

                    _newPageAllocator.FreePage(pageNumber);
                }
                else
                {
                    if (_isIndexTree)
                        Tree.ThrowAttemptToFreeIndexPageToFreeSpaceHandling(Name, pageNumber);

                    _tx.FreePage(pageNumber);
                }
            }
        }

        private FixedSizeTreePage ModifyPage(FixedSizeTreePage page)
        {
            if (page.Dirty)
                return page;

            var writablePage = _tx.ModifyPage(page.PageNumber);
            var newPage = new FixedSizeTreePage(writablePage.Pointer, _entrySize, Constants.Storage.PageSize)
            {
                LastSearchPosition = page.LastSearchPosition,
                LastMatch = page.LastMatch
            };

            return newPage;
        }

        private FixedSizeTreePage PageSplit(FixedSizeTreePage page, long key)
        {
            FixedSizeTreePage parentPage = _cursor.Count > 0 ? _cursor.Pop() : null;
            if (parentPage == null) // root split
            {
                parentPage = NewPage(FixedSizeTreePageFlags.Branch, page.PageNumber);
                parentPage.NumberOfEntries = 1;
                parentPage.StartPosition = (ushort)Constants.FixedSizeTree.PageHeaderSize;
                parentPage.ValueSize = _valSize;

                using (ModifyLargeHeader(out var largePtr))
                {
                    largePtr->RootPageNumber = parentPage.PageNumber;
                    largePtr->Depth++;
                    largePtr->PageCount++;
                }

                var entry = parentPage.GetEntry(0);
                entry->Key = long.MinValue;
                entry->PageNumber = page.PageNumber;
            }

            parentPage = ModifyPage(parentPage);
            if (page.IsLeaf) // simple case of splitting a leaf pageNum
            {
                var newPage = NewPage(FixedSizeTreePageFlags.Leaf, page.PageNumber);
                newPage.StartPosition = (ushort)Constants.FixedSizeTree.PageHeaderSize;
                newPage.ValueSize = _valSize;
                newPage.NumberOfEntries = 0;

                long separatorKey;
                FixedSizeTreeHeader.Large* largePtr;
                using (ModifyLargeHeader(out largePtr))
                {
                    largePtr->PageCount++;

                    // need to add past end of pageNum, optimized
                    if (page.LastSearchPosition >= page.NumberOfEntries)
                    {
                        AddLeafKey(newPage, 0, key);
                        largePtr->NumberOfEntries++;

                        separatorKey = key;
                    }
                    else // not at end, random inserts, split page 3/4 to 1/4
                    {
                        var entriesToMove = (ushort)(page.NumberOfEntries / 4);
                        newPage.NumberOfEntries = entriesToMove;
                        page.NumberOfEntries -= entriesToMove;
                        Memory.Copy(newPage.Pointer + newPage.StartPosition,
                            page.Pointer + page.StartPosition + (page.NumberOfEntries * _entrySize),
                            newPage.NumberOfEntries * _entrySize
                        );

                        separatorKey = newPage.GetKey(0);
                    }
                }

                AddSeparatorToParentPage(parentPage, parentPage.LastSearchPosition + 1, separatorKey, newPage.PageNumber);
                return null; // we don't care about it for leaf pages
            }
            else // branch page
            {
                var newPage = NewPage(FixedSizeTreePageFlags.Branch, page.PageNumber);
                newPage.StartPosition = (ushort)Constants.FixedSizeTree.PageHeaderSize;
                newPage.ValueSize = _valSize;
                newPage.NumberOfEntries = 0;

                FixedSizeTreeHeader.Large* largePtr;
                using (ModifyLargeHeader(out largePtr))
                {
                    largePtr->PageCount++;
                }

                if (page.LastMatch > 0)
                    page.LastSearchPosition++;

                // need to add past end of pageNum, optimized
                if (page.LastSearchPosition >= page.NumberOfEntries)
                {
                    // here we steal the last entry from the current page so we maintain the implicit null left entry
                    var entry = newPage.GetEntry(0);
                    *entry = *page.GetEntry(page.NumberOfEntries - 1);

                    newPage.NumberOfEntries++;
                    page.NumberOfEntries--;

                    AddSeparatorToParentPage(parentPage, parentPage.LastSearchPosition + 1, entry->Key,
                        newPage.PageNumber);

                    return newPage; // this is where the new entry needs to go
                }
                // not at end, random inserts, split page 3/4 to 1/4

                var entriesToMove = (ushort)(page.NumberOfEntries / 4);
                newPage.NumberOfEntries = entriesToMove;
                page.NumberOfEntries -= entriesToMove;
                Memory.Copy(newPage.Pointer + newPage.StartPosition,
                    page.Pointer + page.StartPosition + (page.NumberOfEntries * BranchEntrySize),
                    newPage.NumberOfEntries * BranchEntrySize
                    );

                var newKey = newPage.GetKey(0);

                AddSeparatorToParentPage(parentPage, parentPage.LastSearchPosition + 1, newKey, newPage.PageNumber);

                return (newKey > key) ? page : newPage;
            }
        }

        private void AddLeafKey(FixedSizeTreePage page, int position, long key)
        {
            page.SetKey(key, position);
            page.NumberOfEntries++;
        }

        private void AddSeparatorToParentPage(FixedSizeTreePage parentPage, int position, long key, long pageNum)
        {
            if ((parentPage.NumberOfEntries + 1) * BranchEntrySize > parentPage.PageMaxSpace)
            {
                parentPage = PageSplit(parentPage, key);
                System.Diagnostics.Debug.Assert(parentPage != null);
                BinarySearch(parentPage, key);
                position = parentPage.LastSearchPosition;
                if (parentPage.LastMatch > 0)
                    position++;
            }

            var entriesToMove = parentPage.NumberOfEntries - (position);
            parentPage.ResetStartPosition();
            var newEntryPos = parentPage.Pointer + parentPage.StartPosition + (position * BranchEntrySize);
            if (entriesToMove > 0)
            {
                Memory.Move(newEntryPos + BranchEntrySize,
                    newEntryPos,
                    entriesToMove * BranchEntrySize);
            }
            parentPage.NumberOfEntries++;

            var newEntry = (FixedSizeTreeEntry*)newEntryPos;
            newEntry->Key = key;
            newEntry->PageNumber = pageNum;
        }

        private byte* AddEmbeddedEntry(long key, out bool isNew)
        {
            TemporaryPage tmp;
            using (_tx.Environment.GetTemporaryPage(_tx, out tmp))
            {
                int newSize;
                int srcCopyStart;
                var newEntriesCount = CopyEmbeddedContentToTempPage(key, tmp, out isNew, out newSize, out srcCopyStart);

                if (newEntriesCount > _maxEmbeddedEntries)
                {
                    // convert to large database

                    var allocatePage = NewPage(FixedSizeTreePageFlags.Leaf, 0);

                    FixedSizeTreeHeader.Large* largeHeader;
                    using (ModifyLargeHeader(out largeHeader))
                    {
                        largeHeader->NumberOfEntries = newEntriesCount;
                        largeHeader->ValueSize = _valSize;
                        largeHeader->Depth = 1;
                        largeHeader->RootObjectType = RootObjectType.FixedSizeTree;
                        largeHeader->RootPageNumber = allocatePage.PageNumber;
                        largeHeader->PageCount = 1;

                        allocatePage.FixedTreeFlags = FixedSizeTreePageFlags.Leaf;
                        allocatePage.PageNumber = allocatePage.PageNumber;
                        allocatePage.NumberOfEntries = newEntriesCount;
                        allocatePage.ValueSize = _valSize;
                        allocatePage.StartPosition = (ushort)Constants.FixedSizeTree.PageHeaderSize;
                        Memory.Copy(allocatePage.Pointer + allocatePage.StartPosition, tmp.TempPagePointer,
                            newSize);

                        return allocatePage.Pointer + allocatePage.StartPosition + srcCopyStart + sizeof(long);
                    }
                }

                byte* newData;
                using (_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Embedded) + newSize, out newData))
                {
                    var header = (FixedSizeTreeHeader.Embedded*)newData;
                    header->ValueSize = _valSize;
                    header->RootObjectType = RootObjectType.EmbeddedFixedSizeTree;
                    header->NumberOfEntries = newEntriesCount;

                    Memory.Copy(newData + sizeof(FixedSizeTreeHeader.Embedded), tmp.TempPagePointer,
                        newSize);

                    return newData + sizeof(FixedSizeTreeHeader.Embedded) + srcCopyStart + sizeof(long);
                }
            }
        }

        private ushort CopyEmbeddedContentToTempPage(long key, TemporaryPage tmp, out bool isNew, out int newSize, out int srcCopyStart)
        {
            var ptr = _parent.DirectRead(_treeName);
            if (ptr == null)
            {
                // we called NewPage and emptied this completed, then called CopyEmbeddedContentToTempPage() on effectively empty
                isNew = true;
                newSize = _entrySize;
                srcCopyStart = 0;
                *((long*)tmp.TempPagePointer) = key;

                return 1;
            }
            var dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
            var header = (FixedSizeTreeHeader.Embedded*)ptr;
            var startingEntryCount = header->NumberOfEntries;
            var pos = BinarySearch(dataStart, startingEntryCount, key, _entrySize);
            var newEntriesCount = startingEntryCount;
            isNew = _lastMatch != 0;
            if (isNew)
            {
                newEntriesCount++; // new entry, need more space
            }
            if (_lastMatch > 0)
                pos++; // we need to put this _after_ the previous one
            newSize = (newEntriesCount * _entrySize);

            srcCopyStart = pos * _entrySize;

            if (_lastMatch == 0)
            {
                // can just copy as is
                Memory.Copy(tmp.TempPagePointer, dataStart, startingEntryCount * _entrySize);
            }
            else
            {
                // copy with a gap
                Memory.Copy(tmp.TempPagePointer, dataStart, srcCopyStart);
                var sizeLeftToCopy = (startingEntryCount - pos) * _entrySize;
                if (sizeLeftToCopy > 0)
                {
                    Memory.Copy(tmp.TempPagePointer + srcCopyStart + _entrySize,
                        dataStart + srcCopyStart, sizeLeftToCopy);
                }
            }


            var newEntryStart = tmp.TempPagePointer + srcCopyStart;
            *((long*)newEntryStart) = key;

            return newEntriesCount;
        }

        private byte* AddNewEntry(long key)
        {
            // new, just create it & go
            byte* ptr;
            using (_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Embedded) + _entrySize, out ptr))
            {
                var header = (FixedSizeTreeHeader.Embedded*)ptr;
                header->RootObjectType = RootObjectType.EmbeddedFixedSizeTree;
                header->ValueSize = _valSize;
                header->NumberOfEntries = 1;

                byte* dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
                *(long*)(dataStart) = key;
                return (dataStart + sizeof(long));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BinarySearch(FixedSizeTreePage page, long val)
        {
            page.LastSearchPosition = BinarySearch(page.Pointer + page.StartPosition, page.NumberOfEntries, val, page.IsLeaf ? _entrySize : BranchEntrySize);
            page.LastMatch = _lastMatch;
        }

        private int _lastMatch;
        private int _directAddUsage;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int BinarySearch(byte* p, int len, long val, int size)
        {
            int low = 0;
            int high = len - 1;

            int position = 0;
            int lastMatch = _lastMatch;
            while (low <= high)
            {
                position = (low + high) >> 1;
                var curKey = FixedSizeTreePage.GetEntry(p, position, size)->Key;
                lastMatch = val.CompareTo(curKey);
                if (lastMatch == 0)
                    break;

                if (lastMatch > 0)
                    low = position + 1;
                else
                    high = position - 1;
            }
            _lastMatch = lastMatch;
            return position;
        }


        public List<long> AllPages()
        {
            var results = new List<long>();
            switch (Type)
            {
                case null:
                    break;
                case RootObjectType.EmbeddedFixedSizeTree:
                    break;
                case RootObjectType.FixedSizeTree:
                    var largePtr = (FixedSizeTreeHeader.Large*)_parent.DirectRead(_treeName);
                    var root = GetReadOnlyPage(largePtr->RootPageNumber);

                    var stack = new Stack<FixedSizeTreePage>();
                    stack.Push(root);

                    while (stack.Count > 0)
                    {
                        var p = stack.Pop();
                        results.Add(p.PageNumber);

                        if (p.IsBranch)
                        {
                            for (int j= p.NumberOfEntries - 1; j >= 0; j--)
                            {
                                var chhildNumber = p.GetEntry(j)->PageNumber;
                                stack.Push(GetReadOnlyPage(chhildNumber));
                            }
                        }
                    }

                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type?.ToString());
            }

            return results;
        }

        public bool Contains(long key)
        {
            byte* dataStart;
            switch (Type)
            {
                case null:
                    return false;
                case RootObjectType.EmbeddedFixedSizeTree:
                    var embeddedPtr = _parent.DirectRead(_treeName);
                    var header = (FixedSizeTreeHeader.Embedded*)embeddedPtr;
                    dataStart = embeddedPtr + sizeof(FixedSizeTreeHeader.Embedded);
                    BinarySearch(dataStart, header->NumberOfEntries, key, _entrySize);
                    return _lastMatch == 0;
                case RootObjectType.FixedSizeTree:
                    var largePtr = (FixedSizeTreeHeader.Large*)_parent.DirectRead(_treeName);
                    var page = GetReadOnlyPage(largePtr->RootPageNumber);

                    while (page.IsLeaf == false)
                    {
                        BinarySearch(page, key);
                        if (page.LastMatch < 0 && page.LastSearchPosition > 0)
                            page.LastSearchPosition--;
                        var childPageNumber = page.GetEntry(page.LastSearchPosition)->PageNumber;
                        page = GetReadOnlyPage(childPageNumber);
                    }
                    dataStart = page.Pointer + page.StartPosition;

                    BinarySearch(dataStart, page.NumberOfEntries, key, _entrySize);
                    return _lastMatch == 0;
                default:
                    throw new ArgumentOutOfRangeException(Type?.ToString());
            }
        }

        public DeletionResult Delete(long key)
        {
            if (_tx.Flags == TransactionFlags.Read)
                throw new InvalidOperationException("Cannot delete a value in a read only transaction");

            _changes++;
            switch (Type)
            {
                case null:
                    // nothing to do
                    return new DeletionResult();
                case RootObjectType.EmbeddedFixedSizeTree:
                    return RemoveEmbeddedEntry(key);
                case RootObjectType.FixedSizeTree:
                    return RemoveLargeEntry(key);
                default:
                    throw new ArgumentOutOfRangeException(Type?.ToString());
            }

        }

        public struct DeletionResult
        {
            public long NumberOfEntriesDeleted;
            public bool TreeRemoved;
        }

        public DeletionResult DeleteRange(long start, long end)
        {
            if (_tx.Flags == TransactionFlags.Read)
                throw new InvalidOperationException("Cannot delete a range in a read only transaction");

            _changes++;
            if (start > end)
                throw new InvalidOperationException("Start range cannot be greater than the end of the range");

            long entriedDeleted;
            switch (Type)
            {
                case null:
                    entriedDeleted = 0;
                    break;
                case RootObjectType.EmbeddedFixedSizeTree:
                    entriedDeleted = DeleteRangeEmbedded(start, end);
                    break;
                case RootObjectType.FixedSizeTree:
                    entriedDeleted = DeleteRangeLarge(start, end);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type?.ToString());
            }
            return new DeletionResult
            {
                NumberOfEntriesDeleted = entriedDeleted,
                TreeRemoved = Type == null
            };
        }

        private long DeleteRangeEmbedded(long start, long end)
        {
            byte* ptr = _parent.DirectRead(_treeName);
            var header = (FixedSizeTreeHeader.Embedded*)ptr;
            var startingEntryCount = header->NumberOfEntries;
            var startPos = BinarySearch(ptr + sizeof(FixedSizeTreeHeader.Embedded), startingEntryCount, start, _entrySize);
            if (_lastMatch > 0)
                startPos++;
            var endPos = BinarySearch(ptr + sizeof(FixedSizeTreeHeader.Embedded), startingEntryCount, end, _entrySize);
            if (_lastMatch < 0)
                endPos--;

            if (startPos > endPos)
                return 0;

            byte entriesDeleted = (byte)(endPos - startPos + 1);

            if (entriesDeleted == header->NumberOfEntries)
            {
                _parent.Delete(_treeName);
                return entriesDeleted;
            }

            using (_tx.Environment.GetTemporaryPage(_tx, out TemporaryPage tmp))
            {
                int srcCopyStart = startPos * _entrySize + sizeof(FixedSizeTreeHeader.Embedded);

                Memory.Copy(tmp.TempPagePointer, ptr, srcCopyStart);
                Memory.Copy(tmp.TempPagePointer + srcCopyStart, ptr + srcCopyStart + (_entrySize * entriesDeleted), (startingEntryCount - endPos) * _entrySize);

                int newDataSize = sizeof(FixedSizeTreeHeader.Embedded) + ((startingEntryCount - entriesDeleted) * _entrySize);

                byte* newData;
                using (_parent.DirectAdd(_treeName, newDataSize, out newData))
                {
                    Memory.Copy(newData, tmp.TempPagePointer, newDataSize);

                    header = (FixedSizeTreeHeader.Embedded*)newData;
                    header->NumberOfEntries -= entriesDeleted;
                    header->ValueSize = _valSize;
                    header->RootObjectType = RootObjectType.EmbeddedFixedSizeTree;
                }
            }

            return entriesDeleted;
        }

        private long DeleteRangeLarge(long start, long end)
        {
            /*
             * We use the following logic here:
             * - Find the start page, then find the next page to its right.
             * - If the next page's last value is smaller than the end, remove the page
             * - Now we have to rebalance the tree. Doing so may cause the structure of the tree to change,
             *   so we need to find the start page again.
             * - We need special handling for the end node and for the start node only.
             */
            long entriesDeleted = 0;
            FixedSizeTreePage page;
            FixedSizeTreeHeader.Large* largeHeader;

            while (true)
            {
                page = FindPageFor(start);
                if (page.LastMatch > 0)
                    page.LastSearchPosition++;
                if (page.LastSearchPosition < page.NumberOfEntries)
                {
                    var key = page.GetKey(page.LastSearchPosition);
                    if (key > end)
                        return entriesDeleted; // the start is beyond the last end in the tree, done with it
                }

                if (_cursor.Count == 0)
                    break; // single node, no next page to find
                var nextPage = GetNextLeafPage();
                if (nextPage == null)
                    break; // no next page, we are at the end
                var lastKey = nextPage.GetKey(nextPage.NumberOfEntries - 1);
                if (lastKey >= end)
                    break; // we can't delete the entire page, special case handling follows

                entriesDeleted += nextPage.NumberOfEntries;


                using (ModifyLargeHeader(out largeHeader))
                {
                    largeHeader->NumberOfEntries -= nextPage.NumberOfEntries;
                }

                var treeDeleted = RemoveEntirePage(nextPage); // this will rebalance the tree if needed
                System.Diagnostics.Debug.Assert(treeDeleted == false);
            }

            // we now know that the tree contains a maximum of 2 pages with the range
            // now remove the start range from the start page, we do this twice to cover the case
            // where the start & end are on separate pages
            int rangeRemoved = 1;
            while (rangeRemoved > 0 &&
                   Type == RootObjectType.FixedSizeTree // we may revert to embedded by the deletions, or remove entirely
            )
            {
                page = FindPageFor(start);
                if (page.LastMatch > 0)
                    page.LastSearchPosition++;
                if (page.LastSearchPosition < page.NumberOfEntries)
                {
                    var key = page.GetKey(page.LastSearchPosition);
                    if (key > end)
                        break; // we are done
                }
                else // we have no entries to delete on the current page, move to the next one to delete the end range
                {
                    page = GetNextLeafPage();
                    if (page == null)
                        break;
                }

                rangeRemoved = RemoveRangeFromPage(page, end);

                entriesDeleted += rangeRemoved;
            }
            if (Type == RootObjectType.EmbeddedFixedSizeTree)
            {
                // we converted to embeded in the delete, but might still have some range there
                return entriesDeleted + DeleteRangeEmbedded(start, end);
            }

            // note that because we call RebalancePage from RemoveRangeFromPage
            return entriesDeleted;
        }

        private FixedSizeTreePage GetNextLeafPage()
        {
            while (_cursor.Count > 0)
            {
                var page = _cursor.Peek();
                if (++page.LastSearchPosition >= page.NumberOfEntries)
                {
                    _cursor.Pop();
                    continue;
                }

                var nextPageNum = page.GetEntry(page.LastSearchPosition)->PageNumber;
                var childPage = GetReadOnlyPage(nextPageNum);
                if (childPage.IsLeaf)
                    return childPage;
                _cursor.Push(childPage);
            }
            return null;
        }

        private int RemoveRangeFromPage(FixedSizeTreePage page, long rangeEnd)
        {
            page = ModifyPage(page);

            var startPos = page.LastSearchPosition;
            BinarySearch(page, rangeEnd);
            var endPos = page.LastSearchPosition;
            if (page.LastMatch < 0)
                endPos--;
            if (endPos == -1)
                return 0;

            if (startPos == endPos)
            {
                var key = page.GetKey(startPos);
                if (key > rangeEnd)
                    return 0;
            }

            var entriesDeleted = (endPos - startPos + 1);
            if (startPos == 0)
            {
                // if this is the very first item in the page, we can just change the start position
                page.StartPosition += (ushort)(_entrySize * entriesDeleted);
            }
            else
            {
                Memory.Move(page.Pointer + page.StartPosition + (startPos * _entrySize),
                    page.Pointer + page.StartPosition + ((endPos + 1) * _entrySize),
                    ((page.NumberOfEntries - endPos - 1) * _entrySize)
                    );
            }

            page.NumberOfEntries -= (ushort)entriesDeleted;

            FixedSizeTreeHeader.Large* largeHeader;
            using (ModifyLargeHeader(out largeHeader))
                largeHeader->NumberOfEntries -= (ushort)entriesDeleted;

            if (page.NumberOfEntries == 0)
            {
                RemoveEntirePage(page);
                return entriesDeleted;
            }
            if (startPos == 0 && _cursor.Count > 0)
            {
                var parentPage = _cursor.Peek();
                parentPage = ModifyPage(parentPage);
                parentPage.SetKey(page.GetKey(0), parentPage.LastSearchPosition);
            }

            if (page.NumberOfEntries == 0)
            {
                if (RemoveEntirePage(page))
                    return entriesDeleted;
            }
            else
            {
                while (page != null)
                {
                    page = RebalancePage(page);
                }
            }
            return entriesDeleted;
        }

        private bool RemoveEntirePage(FixedSizeTreePage page)
        {
            FreePage(page.PageNumber);

            if (_cursor.Count == 0) //remove the root page
            {
                _parent.Delete(_treeName);
                return true;
            }
            var parentPage = _cursor.Pop();
            parentPage = ModifyPage(parentPage);
            parentPage.RemoveEntry(parentPage.LastSearchPosition);
            while (parentPage != null)
            {
                parentPage = RebalancePage(parentPage);
            }
            return false;
        }


        private DeletionResult RemoveLargeEntry(long key)
        {
            var page = FindPageFor(key);
            if (page.LastMatch != 0)
                return new DeletionResult();

            FixedSizeTreeHeader.Large* largeHeader;
            using (ModifyLargeHeader(out largeHeader))
            {
                largeHeader->NumberOfEntries--;
            }

            page = ModifyPage(page);

            page.RemoveEntry(page.LastSearchPosition);

            while (page != null)
            {
                page = RebalancePage(page);
            }

            ValidateTree();

            return new DeletionResult { NumberOfEntriesDeleted = 1 };
        }

        private FixedSizeTreePage RebalancePage(FixedSizeTreePage page)
        {
            if (_cursor.Count == 0)
            {
                return RebalanceRootPage(page);
            }

            var sizeOfEntryInPage = (page.IsLeaf ? _entrySize : BranchEntrySize);
            var minNumberOfEntriesBeforeRebalance = (Constants.Storage.PageSize / sizeOfEntryInPage) / 4;
            if (page.NumberOfEntries > minNumberOfEntriesBeforeRebalance)
            {
                // if we have more than 25% of the entries that would fit in the page, there is nothing that needs to be done
                // so we are done
                return null;
            }

            // we determined that we require rebalancing...

            var parentPage = _cursor.Pop();
            parentPage = ModifyPage(parentPage);

            if (page.NumberOfEntries == 0)// empty page, delete it and fixup the parent
            {
                // fixup the implicit less than ref
                if (parentPage.LastSearchPosition == 0
                    // if we are 2 or less, we'll remove our entry and the parent page 
                    // page will rebalance in turn, so we shouldn't modify the relevant
                    // entry
                    && parentPage.NumberOfEntries > 2) 
                {
                    parentPage.NumberOfEntries--;
                    // remove the first value
                    parentPage.StartPosition += BranchEntrySize;
                    // set the next value (now the first), to be smaller than everything
                    parentPage.SetKey(long.MinValue, 0);
                }
                else
                {
                    // need to remove from midway through. At any rate, we'll rebalance on next call
                    parentPage.RemoveEntry(parentPage.LastSearchPosition);
                }
                FreePage(page.PageNumber);
                
                return parentPage;
            }

            if (page.IsBranch && page.NumberOfEntries == 1)
            {
                // we can just collapse this to the parent
                // write the page value to the parent

                var entry = parentPage.GetEntry(parentPage.LastSearchPosition);
                // we don't fixup the key here because there is no need and the 
                // actual key used might be different between the current page
                // and its child
                entry->PageNumber = page.GetEntry(0)->PageNumber;

                // then delete the page
                FreePage(page.PageNumber);
                
                return parentPage;
            }

            if (page.IsLeaf && page.LastSearchPosition == 0)
            {
                // special handling for deleting from start of the page
                // we want to make this efficient, so we will not try to merge leaf pages
                // where all the deletions happen on the start. That way, they can be removed
                // without a lot of overhead.
                return null;
            }

            int oldNumberOfPages;
            System.Diagnostics.Debug.Assert(parentPage.NumberOfEntries >= 2);//otherwise this isn't a valid branch page
            if (parentPage.LastSearchPosition == 0)
            {
                // the current page is the leftmost one, so let us try steal some data
                // from the one on the right
                var siblingNum = parentPage.GetEntry(1)->PageNumber;
                var siblingPage = GetReadOnlyPage(siblingNum);
                if (siblingPage.FixedTreeFlags != page.FixedTreeFlags)
                    return null; // we cannot steal from a leaf sibling if we are branch, or vice versa

                siblingPage = ModifyPage(siblingPage);

                if (siblingPage.NumberOfEntries <= minNumberOfEntriesBeforeRebalance * 2)
                {
                    // we can merge both pages into a single one and still have enough over
                    page.ResetStartPosition();
                    Memory.Copy(
                        page.Pointer + page.StartPosition + (page.NumberOfEntries * sizeOfEntryInPage),
                        siblingPage.Pointer + siblingPage.StartPosition,
                        siblingPage.NumberOfEntries * sizeOfEntryInPage
                        );
                    oldNumberOfPages = page.NumberOfEntries;
                    page.NumberOfEntries += siblingPage.NumberOfEntries;
                    // we need to set the leftmost key in the entries we
                    // moved to the key of the page we are going to remove
                    // to avoid smallest being injected into the sibling
                    // page, see RavenDB-9916
                    if (page.IsBranch)
                    {
                        page.SetKey(
                            parentPage.GetKey(parentPage.LastSearchPosition + 1),
                            oldNumberOfPages);
                    }
                    FreePage(siblingNum);

                    // now fix parent ref, in this case, just removing it is enough
                    parentPage.RemoveEntry(1);

                    return parentPage;
                }
                // too big to just merge, let just take half of the sibling and move on
                var entriesToTake = (siblingPage.NumberOfEntries / 2);
                page.ResetStartPosition();
                Memory.Copy(
                    page.Pointer + page.StartPosition + (page.NumberOfEntries * sizeOfEntryInPage),
                    siblingPage.Pointer + siblingPage.StartPosition,
                    entriesToTake * sizeOfEntryInPage
                    );
                oldNumberOfPages = page.NumberOfEntries;
               
                page.NumberOfEntries += (ushort)entriesToTake;
                siblingPage.NumberOfEntries -= (ushort)entriesToTake;
                siblingPage.StartPosition += (ushort)(sizeOfEntryInPage * entriesToTake);

                // we need to set the leftmost key in the entries we
                // moved to the key of the page we are going to remove
                // to avoid smallest being injected into the sibling
                // page, see RavenDB-9916
                if (page.IsBranch)
                {
                    page.SetKey(
                        parentPage.GetKey(parentPage.LastSearchPosition + 1),
                        oldNumberOfPages);
                }
                // now update the new separator in the sibling position in the parent
                var newSeparator = siblingPage.GetKey(0);
                parentPage.SetKey(newSeparator, 1);

                return parentPage;
            }
            else // we aren't the leftmost item, so we will take from the page on our left
            {
                var siblingNum = parentPage.GetEntry(parentPage.LastSearchPosition - 1)->PageNumber;
                var siblingPage = GetReadOnlyPage(siblingNum);
                siblingPage = ModifyPage(siblingPage);
                if (siblingPage.FixedTreeFlags != page.FixedTreeFlags)
                    return null; // we cannot steal from a leaf sibling if we are branch, or vice versa

                if (siblingPage.NumberOfEntries <= minNumberOfEntriesBeforeRebalance * 2)
                {
                    // we can merge both pages into a single one and still have enough over
                    siblingPage.ResetStartPosition();
                    Memory.Copy(
                        siblingPage.Pointer + siblingPage.StartPosition + (siblingPage.NumberOfEntries * sizeOfEntryInPage),
                        page.Pointer + page.StartPosition,
                        page.NumberOfEntries * sizeOfEntryInPage
                        );
                    oldNumberOfPages = siblingPage.NumberOfEntries;
                    siblingPage.NumberOfEntries += page.NumberOfEntries;
                    // we need to set the leftmost key in the entries we
                    // moved to the key of the page we are going to remove
                    // to avoid smallest being injected into the sibling
                    // page, see RavenDB-9916
                    if (siblingPage.IsBranch)
                    {
                        siblingPage.SetKey(
                          parentPage.GetKey(parentPage.LastSearchPosition),
                          oldNumberOfPages);
                    }
                    FreePage(page.PageNumber);

                    // now fix parent ref, in this case, just removing it is enough
                    parentPage.RemoveEntry(parentPage.LastSearchPosition);

                    return parentPage;
                }
                // too big to just merge, let just take half of the sibling and move on
                var entriesToTake = (siblingPage.NumberOfEntries / 2);
                page.ResetStartPosition();

                if (page.IsBranch)
                {
                    // if we are a branch page and we copy items from our left we need to make
                    // sure that the implicit left entry is fixed. We do that by copying the
                    // entry our parent has for us as the leftmost entry for our current state
                    // and then we copy the entries to our left
                    page.SetKey(parentPage.GetKey(parentPage.LastSearchPosition),0);
                }
                
                Memory.Move(page.Pointer + page.StartPosition + (entriesToTake * sizeOfEntryInPage),
                    page.Pointer + page.StartPosition,
                    entriesToTake * sizeOfEntryInPage);

                Memory.Copy(
                    page.Pointer + page.StartPosition,
                    siblingPage.Pointer + siblingPage.StartPosition + ((siblingPage.NumberOfEntries - entriesToTake) * sizeOfEntryInPage),
                    entriesToTake * sizeOfEntryInPage
                    );

                // we don't need to do any fixup of the values themselves, because we take only the rightmost half
                // of the values from the left sibling and that doesn't include the [smallest] value by definition
                // RavenDB-9916

                page.NumberOfEntries += (ushort)entriesToTake;
                siblingPage.NumberOfEntries -= (ushort)entriesToTake;

                // now update the new separator in the parent

                var newSeparator = page.GetKey(0);
                parentPage.SetKey(newSeparator, parentPage.LastSearchPosition);

                return parentPage;
            }
        }

        private  FixedSizeTreePage RebalanceRootPage(FixedSizeTreePage page)
        {
            FixedSizeTreeHeader.Large* largeHeader;

            if (page.IsBranch)
            {
                // can't proceed here
                if (page.NumberOfEntries != 1)
                    return null;
                // can just replace the child and use my own page
                var childPage = page.GetEntry(0)->PageNumber;
                var rootPageNum = page.PageNumber;
                Memory.Copy(page.Pointer, GetReadOnlyPage(childPage).Pointer, Constants.Storage.PageSize);
                page.PageNumber = rootPageNum; //overwritten by copy

                using (ModifyLargeHeader(out largeHeader))
                {
                    largeHeader->Depth--;
                }

                FreePage(childPage);
                return page;
            }

            // here we are a leaf root page
            largeHeader = (FixedSizeTreeHeader.Large*)_parent.DirectRead(_treeName);

            if (largeHeader->NumberOfEntries <= _maxEmbeddedEntries)
            {
                System.Diagnostics.Debug.Assert(page.IsLeaf);
                System.Diagnostics.Debug.Assert(page.NumberOfEntries == largeHeader->NumberOfEntries);

                // and small enough to fit, converting to embedded
                using (_parent.DirectAdd(_treeName,
                    sizeof(FixedSizeTreeHeader.Embedded) + (_entrySize * page.NumberOfEntries),
                    out var ptr))
                {
                    var header = (FixedSizeTreeHeader.Embedded*)ptr;
                    header->RootObjectType = RootObjectType.EmbeddedFixedSizeTree;
                    header->ValueSize = _valSize;
                    header->NumberOfEntries = (byte)page.NumberOfEntries;

                    Memory.Copy(ptr + sizeof(FixedSizeTreeHeader.Embedded),
                        page.Pointer + page.StartPosition,
                        (_entrySize * page.NumberOfEntries));
                }

                // side affect, this updates the number of pages in the 
                // large fixed size tree header, so we disable this to avoid 
                // reverting to an large (and empty) fixed size tree
                FreePage(page.PageNumber, modifyPageCount: false);
            }
            return null;
        }

        private DeletionResult RemoveEmbeddedEntry(long key)
        {
            byte* ptr = _parent.DirectRead(_treeName);
            var header = (FixedSizeTreeHeader.Embedded*)ptr;
            var startingEntryCount = header->NumberOfEntries;
            var pos = BinarySearch(ptr + sizeof(FixedSizeTreeHeader.Embedded), startingEntryCount, key, _entrySize);
            if (_lastMatch != 0)
            {
                return new DeletionResult(); // not here, nothing to do
            }
            if (startingEntryCount == 1)
            {
                // only single entry, just remove it
                _parent.Delete(_treeName);
                return new DeletionResult { NumberOfEntriesDeleted = 1, TreeRemoved = true };
            }

            TemporaryPage tmp;
            using (_tx.Environment.GetTemporaryPage(_tx, out tmp))
            {
                int srcCopyStart = pos * _entrySize + sizeof(FixedSizeTreeHeader.Embedded);
                Memory.Copy(tmp.TempPagePointer, ptr, srcCopyStart);
                Memory.Copy(tmp.TempPagePointer + srcCopyStart, ptr + srcCopyStart + _entrySize, (header->NumberOfEntries - pos - 1) * _entrySize);

                var newDataSize = sizeof(FixedSizeTreeHeader.Embedded) + ((startingEntryCount - 1) * _entrySize);

                byte* addPtr;
                using (_parent.DirectAdd(_treeName, newDataSize, out addPtr))
                {
                    Memory.Copy(addPtr, tmp.TempPagePointer, newDataSize);

                    header = (FixedSizeTreeHeader.Embedded*)addPtr;
                    header->NumberOfEntries--;
                    header->ValueSize = _valSize;
                    header->RootObjectType = RootObjectType.EmbeddedFixedSizeTree;
                }
                return new DeletionResult { NumberOfEntriesDeleted = 1 };
            }
        }

        public ByteStringContext.ExternalScope Read(long key, out Slice slice)
        {
            switch (Type)
            {
                case null:
                    slice = new Slice();
                    return new ByteStringContext<ByteStringMemoryCache>.ExternalScope();

                case RootObjectType.EmbeddedFixedSizeTree:
                    var ptr = _parent.DirectRead(_treeName);
                    var header = (FixedSizeTreeHeader.Embedded*)ptr;
                    var dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
                    var pos = BinarySearch(dataStart, header->NumberOfEntries, key, _entrySize);
                    if (_lastMatch != 0)
                        goto case null;

                    return Slice.External(_tx.Allocator, dataStart + (pos * _entrySize) + sizeof(long), _valSize, out slice);

                case RootObjectType.FixedSizeTree:
                    var largePtr = (FixedSizeTreeHeader.Large*)_parent.DirectRead(_treeName);

                    var page = GetReadOnlyPage(largePtr->RootPageNumber);
                    while (page.IsLeaf == false)
                    {
                        BinarySearch(page, key);
                        if (page.LastMatch < 0 && page.LastSearchPosition > 0)
                            page.LastSearchPosition--;
                        var childPageNumber = page.GetEntry(page.LastSearchPosition)->PageNumber;
                        page = GetReadOnlyPage(childPageNumber);
                    }
                    dataStart = page.Pointer + page.StartPosition;

                    BinarySearch(page, key);
                    if (_lastMatch != 0)
                        goto case null;

                    return Slice.External(_tx.Allocator, dataStart + (page.LastSearchPosition * _entrySize) + sizeof(long), _valSize, out slice);

                default:
                    throw new ArgumentOutOfRangeException(Type?.ToString());
            }
        }

        public IFixedSizeIterator Iterate()
        {
            switch (Type)
            {
                case null:
                    return new NullIterator();
                case RootObjectType.EmbeddedFixedSizeTree:
                    return new EmbeddedIterator(this);
                case RootObjectType.FixedSizeTree:
                    return new LargeIterator(this);
                default:
                    throw new ArgumentOutOfRangeException(Type?.ToString());
            }
        }

        public long NumberOfEntries
        {
            get
            {
                var header = _parent.DirectRead(_treeName);
                if (header == null)
                    return 0;

                var flags = ((FixedSizeTreeHeader.Embedded*)header)->RootObjectType;
                switch (flags)
                {
                    case RootObjectType.EmbeddedFixedSizeTree:
                        return ((FixedSizeTreeHeader.Embedded*)header)->NumberOfEntries;
                    case RootObjectType.FixedSizeTree:
                        return ((FixedSizeTreeHeader.Large*)header)->NumberOfEntries;
                    default:
                        throw new ArgumentOutOfRangeException(Type?.ToString());
                }
            }
        }


        public long PageCount
        {
            get
            {
                var header = _parent.DirectRead(_treeName);
                if (header == null)
                    return 0;

                var flags = ((FixedSizeTreeHeader.Embedded*)header)->RootObjectType;
                switch (flags)
                {
                    case RootObjectType.EmbeddedFixedSizeTree:
                        return 1;
                    case RootObjectType.FixedSizeTree:
                        return ((FixedSizeTreeHeader.Large*)header)->PageCount;
                    default:
                        throw new ArgumentOutOfRangeException(Type?.ToString());
                }
            }
        }

        public bool FreeSpaceTree { get; set; }
        public Tree Parent => _parent;
        public ushort ValueSize => _valSize;

        public int Depth
        {
            get
            {
                var header = _parent.DirectRead(_treeName);
                if (header == null)
                    return 0;

                var flags = ((FixedSizeTreeHeader.Embedded*)header)->RootObjectType;
                switch (flags)
                {
                    case RootObjectType.EmbeddedFixedSizeTree:
                        return 0;
                    case RootObjectType.FixedSizeTree:
                        return ((FixedSizeTreeHeader.Large*)header)->Depth;
                    default:
                        throw new ArgumentOutOfRangeException(Type?.ToString());
                }
            }
        }

        [Conditional("DEBUG")]
        public void DebugRenderAndShow()
        {
            DebugStuff.RenderAndShow_FixedSizeTree(_tx, this);
        }

        public void Dispose()
        {
        }

        private Tree.DirectAddScope ModifyLargeHeader(out FixedSizeTreeHeader.Large* largeHeader)
        {
            var largeHeaderScope = _parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large), out var ptr);

            largeHeader = (FixedSizeTreeHeader.Large*)ptr;

            return largeHeaderScope;
        }

        public override string ToString()
        {
            return Name.ToString();
        }

        public long GetNumberOfEntriesAfter(long value, out long totalCount)
        {
            totalCount = NumberOfEntries;
            if (totalCount == 0)
                return 0;

            if (Type.HasValue == false)
                return 0;

            if (Type.Value == RootObjectType.EmbeddedFixedSizeTree)
            {
                using (var it = (EmbeddedIterator)Iterate())
                {
                    if (it.Seek(value) == false)
                        return 0;

                    return it.NumberOfEntriesLeft;
                }
            }

            var page = FindPageFor(value);
            if (page.LastMatch >= 0)
                page.LastSearchPosition++;

            var count = GetRemainingNumberOfEntriesFor(page);

            while (_cursor.TryPop(out page))
            {
                System.Diagnostics.Debug.Assert(page.IsBranch);

                page.LastSearchPosition++;
                count += GetRemainingNumberOfEntriesFor(page);
            }

            return count;
        }

        private long GetRemainingNumberOfEntriesFor(FixedSizeTreePage page)
        {
            if (page.IsLeaf)
                return page.NumberOfEntries - page.LastSearchPosition;

            if (page.IsBranch)
            {
                long count = 0;
                while (page.LastSearchPosition < page.NumberOfEntries)
                {
                    var entry = page.GetEntry(page.LastSearchPosition);
                    var childPage = GetReadOnlyPage(entry->PageNumber);

                    count += GetRemainingNumberOfEntriesFor(childPage);

                    page.LastSearchPosition++;
                }

                return count;
            }

            throw new InvalidOperationException("Should not happen!");
        }

        internal void SetNewPageAllocator(NewPageAllocator newPageAllocator)
        {
            System.Diagnostics.Debug.Assert(newPageAllocator != null);

            _newPageAllocator = newPageAllocator;
        }
    }
}
