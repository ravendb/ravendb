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
using Voron.Data.BTrees;
using Voron.Debugging;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.Fixed
{
    public unsafe partial class FixedSizeTree : IDisposable
    {
        internal const int BranchEntrySize = sizeof(long) + sizeof(long);
        private readonly LowLevelTransaction _tx;
        private readonly Tree _parent;
        private Slice _treeName;
        private readonly ushort _valSize;
        private readonly int _entrySize;
        private readonly int _maxEmbeddedEntries;
        private PageLocator _pageLocator;
        private RootObjectType? _type;
        private Stack<FixedSizeTreePage> _cursor;
        private int _changes;

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

        public void RepurposeInstance(Slice treeName, bool clone)
        {
            if (clone)
            {
                if(_treeName .HasValue)
                    _tx.Allocator.Release(ref _treeName.Content);

                _treeName = treeName.Clone(_tx.Allocator);
            }
            else
            {
                _treeName = treeName;
            }

            _type = null;

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

            _type = header->RootObjectType;

            if (header->ValueSize != _valSize)
                ThrowInvalidFixedSizeTreeSize(header);
        }

        private void ThrowInvalidFixedSizeTreeSize(FixedSizeTreeHeader.Embedded* header)
        {
            throw new InvalidOperationException("The expected value len " + _valSize + " does not match actual value len " +
                                                header->ValueSize + " for " + _treeName);
        }

        private static void ThrowInvalidFixedSizeTree(Slice treeName, FixedSizeTreeHeader.Embedded* header)
        {
            throw new ArgumentOutOfRangeException("Tried to open '" + treeName + "' as FixedSizeTree, but is actually " +
                                                  header->RootObjectType);
        }

        public FixedSizeTree(LowLevelTransaction tx, Tree parent, Slice treeName, ushort valSize, bool clone = true)
        {
            _tx = tx;
            _parent = parent;
            _valSize = valSize;
            _pageLocator = tx.PersistentContext.AllocatePageLocator(tx);

            _entrySize = sizeof(long) + _valSize;
            _maxEmbeddedEntries = 512 / _entrySize;
            if (_maxEmbeddedEntries == 0)
                throw new ArgumentException("The value size must be ");

            RepurposeInstance(treeName, clone);
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
            var pos = DirectAdd(key, out isNew);
            if (val.HasValue && val.Size != 0)
                val.CopyTo(pos);

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

        public byte* DirectAdd(long key, out bool isNew)
        {
            _changes++;
            byte* pos;
            switch (_type)
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
                    throw new ArgumentOutOfRangeException(_type.ToString());
            }
            return pos;
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

            var headerToWrite = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));

            ResetStartPosition(page);

            var entriesToMove = page.NumberOfEntries - page.LastSearchPosition;
            if (entriesToMove > 0)
            {
                UnmanagedMemory.Move(page.Pointer + page.StartPosition + ((page.LastSearchPosition + 1) * _entrySize),
                    page.Pointer + page.StartPosition + (page.LastSearchPosition * _entrySize),
                    entriesToMove * _entrySize);
            }

            page.NumberOfEntries++;
            headerToWrite->NumberOfEntries++;

            isNew = true;
            *((long*)(page.Pointer + page.StartPosition + (page.LastSearchPosition * _entrySize))) = key;

            ValidateTree();

            return (page.Pointer + page.StartPosition + (page.LastSearchPosition * _entrySize) + sizeof(long));
        }

        [Conditional("VALIDATE")]
        private void ValidateTree()
        {
            if (_type != RootObjectType.FixedSizeTree)
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

                var prev = KeyFor(cur, 0);
                if (cur.IsBranch)
                    stack.Push(GetReadOnlyPage(PageValueFor(cur, 0)));
                else
                    numberOfEntriesInTree++;

                for (int i = 1; i < cur.NumberOfEntries; i++)
                {
                    var curKey = KeyFor(cur, i);
                    if (prev >= curKey)
                        throw new InvalidOperationException($"Page {cur.PageNumber} is not sorted");

                    if (cur.IsBranch)
                        stack.Push(GetReadOnlyPage(PageValueFor(cur, i)));
                    else
                        numberOfEntriesInTree++;
                }
            }

            if (numberOfEntriesInTree != header->NumberOfEntries)
            {
                throw new InvalidOperationException($"Expected number of entries {header->NumberOfEntries}, actual {numberOfEntriesInTree}");
            }
        }

        private void ResetStartPosition(FixedSizeTreePage page)
        {
            if (page.StartPosition == Constants.FixedSizeTreePageHeaderSize)
                return;

            // we need to move it back, then add the new item
            UnmanagedMemory.Move(page.Pointer + Constants.FixedSizeTreePageHeaderSize,
                page.Pointer + page.StartPosition,
                page.NumberOfEntries * (page.IsLeaf ? _entrySize : BranchEntrySize));
            page.StartPosition = (ushort)Constants.FixedSizeTreePageHeaderSize;
        }

        internal FixedSizeTreePage GetReadOnlyPage(long pageNumber)
        {
            var readOnlyPage = _pageLocator.GetReadOnlyPage(pageNumber);
            return new FixedSizeTreePage(readOnlyPage.Pointer, _tx.PageSize);
        }

        private FixedSizeTreePage FindPageFor(long key)
        {
            var header = (FixedSizeTreeHeader.Large*)_parent.DirectRead(_treeName);
            var page = GetReadOnlyPage(header->RootPageNumber);
            if (_cursor == null)
                _cursor = new Stack<FixedSizeTreePage>();
            else
                _cursor.Clear();

            while (page.IsLeaf == false)
            {
                _cursor.Push(page);
                BinarySearch(page, key);
                if (page.LastMatch < 0 && page.LastSearchPosition > 0)
                    page.LastSearchPosition--;
                var childPageNumber = PageValueFor(page, page.LastSearchPosition);
                page = GetReadOnlyPage(childPageNumber);
            }

            BinarySearch(page, key);
            return page;
        }

        private FixedSizeTreePage NewPage(FixedSizeTreePageFlags flags)
        {
            FixedSizeTreePage allocatePage;

            using (FreeSpaceTree ? _tx.Environment.FreeSpaceHandling.Disable() : null)
            {
                // we cannot recursively call free space handling to ensure that we won't modify a section
                // relevant for a page which is currently being changed allocated

                var page = _tx.AllocatePage(1);
                allocatePage = new FixedSizeTreePage(page.Pointer, _tx.PageSize);
            }

            allocatePage.Dirty = true;
            allocatePage.FixedTreeFlags = flags;
            allocatePage.Flags = PageFlags.Single | PageFlags.FixedSizeTreePage;
            return allocatePage;
        }

        private void FreePage(long pageNumber)
        {
            if (FreeSpaceTree)
            {
                // we cannot recursively call free space handling to ensure that we won't modify a section
                // relevant for a page which is currently being freed, so we will free it on tx commit

                _tx.FreePageOnCommit(pageNumber);
            }
            else
            {
                _tx.FreePage(pageNumber);
            }

            _pageLocator.Reset(pageNumber);
        }

        public FixedSizeTreePage ModifyPage(FixedSizeTreePage page)
        {
            if (page.Dirty)
                return page;

            var writablePage = _pageLocator.GetWritablePage(page.PageNumber);
            var newPage = new FixedSizeTreePage(writablePage.Pointer, _tx.PageSize)
            {
                LastSearchPosition = page.LastSearchPosition,
                LastMatch = page.LastMatch
            };

            return newPage;
        }

        private FixedSizeTreePage PageSplit(FixedSizeTreePage page, long key)
        {
            var largePtr = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
            FixedSizeTreePage parentPage = _cursor.Count > 0 ? _cursor.Pop() : null;
            if (parentPage == null) // root split
            {
                parentPage = NewPage(FixedSizeTreePageFlags.Branch);
                parentPage.NumberOfEntries = 1;
                parentPage.StartPosition = (ushort)Constants.FixedSizeTreePageHeaderSize;
                parentPage.ValueSize = _valSize;

                largePtr->RootPageNumber = parentPage.PageNumber;
                largePtr->Depth++;
                largePtr->PageCount++;
                var dataStart = GetSeparatorKeyAtPosition(parentPage, 0);
                dataStart[0] = long.MinValue;
                dataStart[1] = page.PageNumber;

            }

            parentPage = ModifyPage(parentPage);
            if (page.IsLeaf) // simple case of splitting a leaf pageNum
            {
                var newPage = NewPage(FixedSizeTreePageFlags.Leaf);
                newPage.StartPosition = (ushort)Constants.FixedSizeTreePageHeaderSize;
                newPage.ValueSize = _valSize;
                newPage.NumberOfEntries = 0;
                largePtr->PageCount++;

                // need to add past end of pageNum, optimized
                if (page.LastSearchPosition >= page.NumberOfEntries)
                {
                    AddLeafKey(newPage, 0, key);

                    AddSeparatorToParentPage(parentPage, parentPage.LastSearchPosition + 1, key, newPage.PageNumber);

                    largePtr->NumberOfEntries++;
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
                    AddSeparatorToParentPage(parentPage, parentPage.LastSearchPosition + 1, KeyFor(newPage, 0), newPage.PageNumber);
                }
                return null;// we don't care about it for leaf pages
            }
            else // branch page
            {
                var newPage = NewPage(FixedSizeTreePageFlags.Branch);
                newPage.StartPosition = (ushort)Constants.FixedSizeTreePageHeaderSize;
                newPage.ValueSize = _valSize;
                newPage.NumberOfEntries = 0;
                largePtr->PageCount++;

                if (page.LastMatch > 0)
                    page.LastSearchPosition++;

                // need to add past end of pageNum, optimized
                if (page.LastSearchPosition >= page.NumberOfEntries)
                {
                    // here we steal the last entry from the current page so we maintain the implicit null left entry
                    var dataStart = GetSeparatorKeyAtPosition(newPage, 0);
                    dataStart[0] = KeyFor(page, page.NumberOfEntries - 1);
                    dataStart[1] = PageValueFor(page, page.NumberOfEntries - 1);

                    newPage.NumberOfEntries++;
                    page.NumberOfEntries--;

                    AddSeparatorToParentPage(parentPage, parentPage.LastSearchPosition + 1, dataStart[0],
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

                var newKey = KeyFor(newPage, 0);

                AddSeparatorToParentPage(parentPage, parentPage.LastSearchPosition + 1, newKey, newPage.PageNumber);

                return (newKey > key) ? page : newPage;
            }
        }

        private void AddLeafKey(FixedSizeTreePage page, int position, long key)
        {
            SetSeparatorKeyAtPosition(page, key, position);
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
            ResetStartPosition(parentPage);
            var newEntryPos = parentPage.Pointer + parentPage.StartPosition + ((position) * BranchEntrySize);
            if (entriesToMove > 0)
            {
                UnmanagedMemory.Move(newEntryPos + BranchEntrySize,
                    newEntryPos,
                    entriesToMove * BranchEntrySize);
            }
            parentPage.NumberOfEntries++;
            ((long*)newEntryPos)[0] = key;
            ((long*)newEntryPos)[1] = pageNum;
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
                    _type = RootObjectType.FixedSizeTree;

                    var allocatePage = NewPage(FixedSizeTreePageFlags.Leaf);

                    var largeHeader =
                        (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
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
                    allocatePage.StartPosition = (ushort)Constants.FixedSizeTreePageHeaderSize;
                    Memory.Copy(allocatePage.Pointer + allocatePage.StartPosition, tmp.TempPagePointer,
                        newSize);

                    return allocatePage.Pointer + allocatePage.StartPosition + srcCopyStart + sizeof(long);
                }

                byte* newData = _parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Embedded) + newSize);
                var header = (FixedSizeTreeHeader.Embedded*)newData;
                header->ValueSize = _valSize;
                header->RootObjectType = RootObjectType.EmbeddedFixedSizeTree;
                header->NumberOfEntries = newEntriesCount;

                Memory.Copy(newData + sizeof(FixedSizeTreeHeader.Embedded), tmp.TempPagePointer,
                    newSize);

                return newData + sizeof(FixedSizeTreeHeader.Embedded) + srcCopyStart + sizeof(long);
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
            var ptr = _parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Embedded) + _entrySize);
            var header = (FixedSizeTreeHeader.Embedded*)ptr;
            header->RootObjectType = RootObjectType.EmbeddedFixedSizeTree;
            header->ValueSize = _valSize;
            header->NumberOfEntries = 1;
            _type = RootObjectType.EmbeddedFixedSizeTree;

            byte* dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
            *(long*)(dataStart) = key;
            return (dataStart + sizeof(long));
        }

        private void BinarySearch(FixedSizeTreePage page, long val)
        {
            page.LastSearchPosition = BinarySearch(page.Pointer + page.StartPosition,
                page.NumberOfEntries, val,
                page.IsLeaf ? _entrySize : BranchEntrySize);
            page.LastMatch = _lastMatch;
        }

        private int _lastMatch;
        private int BinarySearch(byte* p, int len, long val, int size)
        {
            int low = 0;
            int high = len - 1;

            int position = 0;
            while (low <= high)
            {
                position = (low + high) >> 1;
                var curKey = KeyFor(p, position, size);
                _lastMatch = val.CompareTo(curKey);
                if (_lastMatch == 0)
                    break;

                if (_lastMatch > 0)
                    low = position + 1;
                else
                    high = position - 1;
            }
            return position;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long KeyFor(FixedSizeTreePage page, int num)
        {
            return KeyFor(page.Pointer + page.StartPosition, num, page.IsLeaf ? _entrySize : BranchEntrySize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long KeyFor(byte* p, int num, int size)
        {
            var lp = (long*)(p + (num * size));
            return lp[0];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long PageValueFor(FixedSizeTreePage page, int num)
        {
            //page.Pointer + page.StartPosition + (page.LastSearchPosition * _entrySize) + sizeof(long);
            var lp = (long*)(page.Pointer + page.StartPosition + num * BranchEntrySize + sizeof(long));
            return lp[0];
        }

        public List<long> AllPages()
        {
            var results = new List<long>();
            switch (_type)
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
                            for (int j = 0; j < p.NumberOfEntries; j++)
                            {
                                var chhildNumber = PageValueFor(p, j);
                                stack.Push(GetReadOnlyPage(chhildNumber));
                            }
                        }
                    }

                    break;
                default:
                    throw new ArgumentOutOfRangeException(_type.ToString());
            }

            return results;
        }

        public bool Contains(long key)
        {
            byte* dataStart;
            switch (_type)
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
                        var childPageNumber = PageValueFor(page, page.LastSearchPosition);
                        page = GetReadOnlyPage(childPageNumber);
                    }
                    dataStart = page.Pointer + page.StartPosition;

                    BinarySearch(dataStart, page.NumberOfEntries, key, _entrySize);
                    return _lastMatch == 0;
                default:
                    throw new ArgumentOutOfRangeException(_type.ToString());
            }
        }

        public DeletionResult Delete(long key)
        {
            _changes++;
            switch (_type)
            {
                case null:
                    // nothing to do
                    return new DeletionResult();
                case RootObjectType.EmbeddedFixedSizeTree:
                    return RemoveEmbeddedEntry(key);
                case RootObjectType.FixedSizeTree:
                    return RemoveLargeEntry(key);
                default:
                    throw new ArgumentOutOfRangeException(_type.ToString());
            }

        }

        public struct DeletionResult
        {
            public long NumberOfEntriesDeleted;
            public bool TreeRemoved;
        }

        public DeletionResult DeleteRange(long start, long end)
        {
            _changes++;
            if (start > end)
                throw new InvalidOperationException("Start range cannot be greater than the end of the range");

            long entriedDeleted;
            switch (_type)
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
                    throw new ArgumentOutOfRangeException(_type.ToString());
            }
            return new DeletionResult
            {
                NumberOfEntriesDeleted = entriedDeleted,
                TreeRemoved = _type == null
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
                _type = null;
                return entriesDeleted;
            }

            byte* newData = _parent.DirectAdd(_treeName,
                sizeof(FixedSizeTreeHeader.Embedded) + ((startingEntryCount - entriesDeleted) * _entrySize));

            int srcCopyStart = startPos * _entrySize + sizeof(FixedSizeTreeHeader.Embedded);

            Memory.Copy(newData, ptr, srcCopyStart);
            Memory.Copy(newData + srcCopyStart, ptr + srcCopyStart + (_entrySize * entriesDeleted), (header->NumberOfEntries - endPos) * _entrySize);

            header = (FixedSizeTreeHeader.Embedded*)newData;
            header->NumberOfEntries -= entriesDeleted;
            header->ValueSize = _valSize;
            header->RootObjectType = RootObjectType.EmbeddedFixedSizeTree;

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
                    var key = KeyFor(page, page.LastSearchPosition);
                    if (key > end)
                        return entriesDeleted; // the start is beyond the last end in the tree, done with it
                }

                if (_cursor.Count == 0)
                    break; // single node, no next page to find
                var nextPage = GetNextLeafPage();
                if (nextPage == null)
                    break; // no next page, we are at the end
                var lastKey = KeyFor(nextPage, nextPage.NumberOfEntries - 1);
                if (lastKey >= end)
                    break; // we can't delete the entire page, special case handling follows

                entriesDeleted += nextPage.NumberOfEntries;
                largeHeader = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
                largeHeader->NumberOfEntries -= nextPage.NumberOfEntries;

                var treeDeleted = RemoveEntirePage(nextPage, largeHeader); // this will rebalance the tree if needed
                System.Diagnostics.Debug.Assert(treeDeleted == false);
            }

            // we now know that the tree contains a maximum of 2 pages with the range
            // now remove the start range from the start page, we do this twice to cover the case
            // where the start & end are on separate pages
            largeHeader = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
            int rangeRemoved = 1;
            while (rangeRemoved > 0 &&
                _type == RootObjectType.FixedSizeTree // we may revert to embedded by the deletions, or remove entirely
                )
            {
                page = FindPageFor(start);
                if (page.LastMatch > 0)
                    page.LastSearchPosition++;
                if (page.LastSearchPosition < page.NumberOfEntries)
                {
                    var key = KeyFor(page, page.LastSearchPosition);
                    if (key > end)
                        break; // we are done
                }
                else // we have no entries to delete on the current page, move to the next one to delete the end range
                {
                    page = GetNextLeafPage();
                    if (page == null)
                        break;
                }

                rangeRemoved = RemoveRangeFromPage(page, end, largeHeader);

                entriesDeleted += rangeRemoved;
            }
            if (_type == RootObjectType.EmbeddedFixedSizeTree)
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

                var nextPageNum = PageValueFor(page, page.LastSearchPosition);
                var childPage = GetReadOnlyPage(nextPageNum);
                if (childPage.IsLeaf)
                    return childPage;
                _cursor.Push(childPage);
            }
            return null;
        }

        private int RemoveRangeFromPage(FixedSizeTreePage page, long rangeEnd, FixedSizeTreeHeader.Large* largeHeader)
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
                var key = KeyFor(page, startPos);
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
                UnmanagedMemory.Move(page.Pointer + page.StartPosition + (startPos * _entrySize),
                    page.Pointer + page.StartPosition + ((endPos + 1) * _entrySize),
                    ((page.NumberOfEntries - endPos - 1) * _entrySize)
                    );
            }

            page.NumberOfEntries -= (ushort)entriesDeleted;
            largeHeader->NumberOfEntries -= (ushort)entriesDeleted;

            if (page.NumberOfEntries == 0)
            {
                RemoveEntirePage(page, largeHeader);
                return entriesDeleted;
            }
            if (startPos == 0 && _cursor.Count > 0)
            {
                var parentPage = _cursor.Peek();
                parentPage = ModifyPage(parentPage);
                SetSeparatorKeyAtPosition(parentPage, KeyFor(page, 0), parentPage.LastSearchPosition);
            }

            if (page.NumberOfEntries == 0)
            {
                if (RemoveEntirePage(page, largeHeader))
                    return entriesDeleted;
            }
            else
            {
                while (page != null)
                {
                    page = RebalancePage(page, largeHeader);
                }
            }
            return entriesDeleted;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long* GetSeparatorKeyAtPosition(FixedSizeTreePage page, int position)
        {
            var dataStart = (long*)(page.Pointer + page.StartPosition + (BranchEntrySize * position));
            return dataStart;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetSeparatorKeyAtPosition(FixedSizeTreePage page, long key, int position)
        {
            var dataStart = GetSeparatorKeyAtPosition(page, position);
            dataStart[0] = key;
        }


        private bool RemoveEntirePage(FixedSizeTreePage page, FixedSizeTreeHeader.Large* largeHeader)
        {
            FreePage(page.PageNumber);
            largeHeader->PageCount--;
            if (_cursor.Count == 0) //remove the root page
            {
                _parent.Delete(_treeName);
                _type = null;
                return true;
            }
            var parentPage = _cursor.Pop();
            parentPage = ModifyPage(parentPage);
            RemoveEntryFromPage(parentPage, parentPage.LastSearchPosition);
            while (parentPage != null)
            {
                parentPage = RebalancePage(parentPage, largeHeader);
            }
            return false;
        }


        private DeletionResult RemoveLargeEntry(long key)
        {
            var page = FindPageFor(key);
            if (page.LastMatch != 0)
                return new DeletionResult();

            var largeHeader = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
            largeHeader->NumberOfEntries--;

            page = ModifyPage(page);

            RemoveEntryFromPage(page, page.LastSearchPosition);

            while (page != null)
            {
                page = RebalancePage(page, largeHeader);
            }

            ValidateTree();

            return new DeletionResult { NumberOfEntriesDeleted = 1 };
        }

        private void RemoveEntryFromPage(FixedSizeTreePage page, int pos)
        {
            System.Diagnostics.Debug.Assert(pos >= 0 && pos < page.NumberOfEntries);
            page.NumberOfEntries--;
            var size = (ushort)(page.IsLeaf ? _entrySize : BranchEntrySize);
            if (pos == 0)
            {
                // optimized, just move the start position
                page.StartPosition += size;
                return;
            }
            // have to move the memory
            UnmanagedMemory.Move(page.Pointer + page.StartPosition + (pos * size),
                   page.Pointer + page.StartPosition + ((pos + 1) * size),
                   (page.NumberOfEntries - pos) * size);
        }

        private FixedSizeTreePage RebalancePage(FixedSizeTreePage page, FixedSizeTreeHeader.Large* largeTreeHeader)
        {
            if (_cursor.Count == 0)
            {
                // root page
                if (largeTreeHeader->NumberOfEntries <= _maxEmbeddedEntries)
                {
                    System.Diagnostics.Debug.Assert(page.IsLeaf);
                    System.Diagnostics.Debug.Assert(page.NumberOfEntries == largeTreeHeader->NumberOfEntries);

                    // and small enough to fit, converting to embedded
                    var ptr = _parent.DirectAdd(_treeName,
                        sizeof(FixedSizeTreeHeader.Embedded) + (_entrySize * page.NumberOfEntries));
                    var header = (FixedSizeTreeHeader.Embedded*)ptr;
                    header->RootObjectType = RootObjectType.EmbeddedFixedSizeTree;
                    header->ValueSize = _valSize;
                    header->NumberOfEntries = (byte)page.NumberOfEntries;
                    _type = RootObjectType.EmbeddedFixedSizeTree;

                    Memory.Copy(ptr + sizeof(FixedSizeTreeHeader.Embedded),
                        page.Pointer + page.StartPosition,
                        (_entrySize * page.NumberOfEntries));

                    FreePage(page.PageNumber);
                }
                else if (page.IsBranch && page.NumberOfEntries == 1)
                {
                    var childPage = PageValueFor(page, 0);
                    var rootPageNum = page.PageNumber;
                    Memory.Copy(page.Pointer, GetReadOnlyPage(childPage).Pointer, _tx.DataPager.PageSize);
                    page.PageNumber = rootPageNum;//overwritten by copy

                    if (largeTreeHeader != null)
                        largeTreeHeader->Depth--;

                    FreePage(childPage);
                    largeTreeHeader->PageCount--;
                }

                return null;
            }


            var sizeOfEntryInPage = (page.IsLeaf ? _entrySize : BranchEntrySize);
            var minNumberOfEntriesBeforeRebalance = (_tx.DataPager.PageMaxSpace / sizeOfEntryInPage) / 4;
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
                if (parentPage.LastSearchPosition == 0 && parentPage.NumberOfEntries > 2)
                {
                    parentPage.NumberOfEntries--;
                    // remove the first value
                    parentPage.StartPosition += BranchEntrySize;
                    // set the next value (now the first), to be smaller than everything
                    SetSeparatorKeyAtPosition(parentPage, long.MinValue, 0);
                }
                else
                {
                    // need to remove from midway through. At any rate, we'll rebalance on next call
                    RemoveEntryFromPage(parentPage, parentPage.LastSearchPosition);
                }
                return parentPage;
            }

            if (page.IsBranch && page.NumberOfEntries == 1)
            {
                // we can just collapse this to the parent
                // write the page value to the parent
                SetSeparatorKeyAtPosition(parentPage, PageValueFor(page, 0), parentPage.LastSearchPosition);
                // then delete the page
                FreePage(page.PageNumber);
                largeTreeHeader->PageCount--;
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

            System.Diagnostics.Debug.Assert(parentPage.NumberOfEntries >= 2);//otherwise this isn't a valid branch page
            if (parentPage.LastSearchPosition == 0)
            {
                // the current page is the leftmost one, so let us try steal some data
                // from the one on the right
                var siblingNum = PageValueFor(parentPage, 1);
                var siblingPage = GetReadOnlyPage(siblingNum);
                if (siblingPage.FixedTreeFlags != page.FixedTreeFlags)
                    return null; // we cannot steal from a leaf sibling if we are branch, or vice versa

                siblingPage = ModifyPage(siblingPage);

                if (siblingPage.NumberOfEntries <= minNumberOfEntriesBeforeRebalance * 2)
                {
                    // we can merge both pages into a single one and still have enough over
                    ResetStartPosition(page);
                    Memory.Copy(
                        page.Pointer + page.StartPosition + (page.NumberOfEntries * sizeOfEntryInPage),
                        siblingPage.Pointer + siblingPage.StartPosition,
                        siblingPage.NumberOfEntries * sizeOfEntryInPage
                        );
                    page.NumberOfEntries += siblingPage.NumberOfEntries;

                    FreePage(siblingNum);
                    largeTreeHeader->PageCount--;

                    // now fix parent ref, in this case, just removing it is enough
                    RemoveEntryFromPage(parentPage, 1);

                    return parentPage;
                }
                // too big to just merge, let just take half of the sibling and move on
                var entriesToTake = (siblingPage.NumberOfEntries / 2);
                ResetStartPosition(page);
                Memory.Copy(
                    page.Pointer + page.StartPosition + (page.NumberOfEntries * sizeOfEntryInPage),
                    siblingPage.Pointer + siblingPage.StartPosition,
                    entriesToTake * sizeOfEntryInPage
                    );
                page.NumberOfEntries += (ushort)entriesToTake;
                siblingPage.NumberOfEntries -= (ushort)entriesToTake;
                siblingPage.StartPosition += (ushort)(sizeOfEntryInPage * entriesToTake);

                // now update the new separator in the sibling position in the parent
                var newSeparator = KeyFor(siblingPage, 0);
                SetSeparatorKeyAtPosition(parentPage, newSeparator, 1);

                return parentPage;
            }
            else // we aren't the leftmost item, so we will take from the page on our left
            {
                var siblingNum = PageValueFor(parentPage, parentPage.LastSearchPosition - 1);
                var siblingPage = GetReadOnlyPage(siblingNum);
                siblingPage = ModifyPage(siblingPage);
                if (siblingPage.FixedTreeFlags != page.FixedTreeFlags)
                    return null; // we cannot steal from a leaf sibling if we are branch, or vice versa

                if (siblingPage.NumberOfEntries <= minNumberOfEntriesBeforeRebalance * 2)
                {
                    // we can merge both pages into a single one and still have enough over
                    ResetStartPosition(siblingPage);
                    Memory.Copy(
                        siblingPage.Pointer + siblingPage.StartPosition + (siblingPage.NumberOfEntries * sizeOfEntryInPage),
                        page.Pointer + page.StartPosition,
                        page.NumberOfEntries * sizeOfEntryInPage
                        );
                    siblingPage.NumberOfEntries += page.NumberOfEntries;

                    FreePage(page.PageNumber);
                    largeTreeHeader->PageCount--;

                    // now fix parent ref, in this case, just removing it is enough
                    RemoveEntryFromPage(parentPage, parentPage.LastSearchPosition);

                    return parentPage;
                }
                // too big to just merge, let just take half of the sibling and move on
                var entriesToTake = (siblingPage.NumberOfEntries / 2);
                ResetStartPosition(page);
                UnmanagedMemory.Move(page.Pointer + page.StartPosition + (entriesToTake * sizeOfEntryInPage),
                    page.Pointer + page.StartPosition,
                    entriesToTake * sizeOfEntryInPage);

                Memory.Copy(
                    page.Pointer + page.StartPosition,
                    siblingPage.Pointer + siblingPage.StartPosition + ((siblingPage.NumberOfEntries - entriesToTake) * sizeOfEntryInPage),
                    entriesToTake * sizeOfEntryInPage
                    );
                page.NumberOfEntries += (ushort)entriesToTake;
                siblingPage.NumberOfEntries -= (ushort)entriesToTake;

                // now update the new separator in the parent

                var newSeparator = KeyFor(page, 0);
                SetSeparatorKeyAtPosition(parentPage, newSeparator, parentPage.LastSearchPosition);


                return parentPage;
            }
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
                _type = null;
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
                byte* newData = _parent.DirectAdd(_treeName, newDataSize);

                Memory.Copy(newData, tmp.TempPagePointer, newDataSize);

                header = (FixedSizeTreeHeader.Embedded*)newData;
                header->NumberOfEntries--;
                header->ValueSize = _valSize;
                header->RootObjectType = RootObjectType.EmbeddedFixedSizeTree;
                return new DeletionResult { NumberOfEntriesDeleted = 1 };
            }
        }

        public ByteStringContext.ExternalScope Read(long key, out Slice slice)
        {
            switch (_type)
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
                        var childPageNumber = PageValueFor(page, page.LastSearchPosition);
                        page = GetReadOnlyPage(childPageNumber);
                    }
                    dataStart = page.Pointer + page.StartPosition;

                    BinarySearch(page, key);
                    if (_lastMatch != 0)
                        goto case null;

                    return Slice.External(_tx.Allocator, dataStart + (page.LastSearchPosition * _entrySize) + sizeof(long), _valSize, out slice);

                default:
                    throw new ArgumentOutOfRangeException(_type.ToString());
            }
        }

        public IFixedSizeIterator Iterate()
        {
            switch (_type)
            {
                case null:
                    return new NullIterator();
                case RootObjectType.EmbeddedFixedSizeTree:
                    return new EmbeddedIterator(this);
                case RootObjectType.FixedSizeTree:
                    return new LargeIterator(this);
                default:
                    throw new ArgumentOutOfRangeException(_type.ToString());
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
                        throw new ArgumentOutOfRangeException(_type.ToString());
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
                        throw new ArgumentOutOfRangeException(_type.ToString());
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
                        throw new ArgumentOutOfRangeException(_type.ToString());
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
            if (_pageLocator != null)
            {
                _tx.PersistentContext.FreePageLocator(_pageLocator);
                _pageLocator = null;
            }
        }
    }
}