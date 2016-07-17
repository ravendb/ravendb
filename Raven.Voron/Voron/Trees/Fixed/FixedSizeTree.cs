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
using Sparrow.Platform;
using Voron.Debugging;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;

namespace Voron.Trees.Fixed
{
    public unsafe partial class FixedSizeTree
    {
        internal const int BranchEntrySize = sizeof(long) + sizeof(long);
        private readonly Transaction _tx;
        private readonly Tree _parent;
        private readonly Slice _treeName;
        private readonly byte _valSize;
        private readonly int _entrySize;
        private readonly int _maxEmbeddedEntries;
        private FixedSizeTreeHeader.OptionFlags? _flags;
        private Stack<Page> _cursor;

        public FixedSizeTree(Transaction tx, Tree parent, Slice treeName, byte valSize)
        {
            _tx = tx;
            _parent = parent;
            _treeName = treeName;
            _valSize = valSize;

            _entrySize = sizeof(long) + _valSize;
            _maxEmbeddedEntries = 512 / _entrySize;

            var header = (FixedSizeTreeHeader.Embedded*)_parent.DirectRead(_treeName);
            if (header == null)
                return;

            _flags = header->Flags;

            if (header->ValueSize != valSize)
                throw new InvalidOperationException("The expected value len " + valSize + " does not match actual value len " +
                                                    header->ValueSize + " for " + _treeName);
        }

        public long[] Debug(Page p)
        {
            var entrySize = _entrySize;
            return Debug(p, entrySize);
        }

        public static long[] Debug(Page p, int entrySize)
        {
            if (p == null)
                return null;
            return Debug(p.Base + p.FixedSize_StartPosition, p.FixedSize_NumberOfEntries,
                p.IsLeaf ? entrySize : BranchEntrySize);
        }

        public Slice Name
        {
            get { return _treeName; }
        }

        public static long[] Debug(byte* p, int entries, int size)
        {
            var a = new long[entries];
            for (int i = 0; i < entries; i++)
            {
                a[i] = *((long*)(p + (size * i)));
            }
            return a;
        }

        public bool Add(long key, Slice val = null)
        {
            if (_valSize == 0 && (val != null && val.Size != 0))
                throw new InvalidOperationException("When the value size is zero, no value can be specified");
            if (_valSize != 0 && val == null)
                throw new InvalidOperationException("When the value size is not zero, the value must be specified");
            if (val != null && val.Size != _valSize)
                throw new InvalidOperationException("The value size must be " + _valSize + " but was " + val.Size);

            bool isNew;
            var pos = DirectAdd(key, out isNew);
            if (val != null && val.Size != 0)
                val.CopyTo(pos);

            return isNew;
        }

        public bool Add(long key, byte[] val)
        {
            return Add(key, new Slice(val));
        }

        public byte* DirectAdd(long key, out bool isNew)
        {
            byte* pos;
            switch (_flags)
            {
                case null:
                    pos = AddNewEntry(key);
                    isNew = true;
                    break;
                case FixedSizeTreeHeader.OptionFlags.Embedded:
                    pos = AddEmbeddedEntry(key, out isNew);
                    break;
                case FixedSizeTreeHeader.OptionFlags.Large:
                    pos = AddLargeEntry(key, out isNew);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            return pos;
        }

        private byte* AddLargeEntry(long key, out bool isNew)
        {
            var page = FindPageFor(key);

            page = _tx.ModifyPage(page.PageNumber, _parent, page);

            if (_lastMatch == 0) // update
            {
                isNew = false;
                return page.Base + page.FixedSize_StartPosition + (page.LastSearchPosition * _entrySize) + sizeof(long);
            }

            if (page.LastMatch > 0)
                page.LastSearchPosition++; // after the last one

            if ((page.FixedSize_NumberOfEntries + 1) * _entrySize > page.PageMaxSpace)
            {
                PageSplit(page, key);

                // now we know we have enough space, or we need to split the parent pageNum
                var addLargeEntry = AddLargeEntry(key, out isNew);
                isNew = true;
                return addLargeEntry;
            }

            var headerToWrite = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));

            ResetStartPosition(page);

            var entriesToMove = page.FixedSize_NumberOfEntries - page.LastSearchPosition;
            if (entriesToMove > 0)
            {
                UnmanagedMemory.Move(page.Base + page.FixedSize_StartPosition + ((page.LastSearchPosition + 1) * _entrySize),
                    page.Base + page.FixedSize_StartPosition + (page.LastSearchPosition * _entrySize),
                    entriesToMove * _entrySize);
            }

            page.FixedSize_NumberOfEntries++;
            headerToWrite->NumberOfEntries++;

            isNew = true;
            *((long*)(page.Base + page.FixedSize_StartPosition + (page.LastSearchPosition * _entrySize))) = key;
            return (page.Base + page.FixedSize_StartPosition + (page.LastSearchPosition * _entrySize) + sizeof(long));
        }

        private void ResetStartPosition(Page page)
        {
            if (page.FixedSize_StartPosition == Constants.PageHeaderSize)
                return;

            // we need to move it back, then add the new item
            UnmanagedMemory.Move(page.Base + Constants.PageHeaderSize,
                page.Base + page.FixedSize_StartPosition,
                page.FixedSize_NumberOfEntries * (page.IsLeaf ? _entrySize : BranchEntrySize));
            page.FixedSize_StartPosition = (ushort)Constants.PageHeaderSize;
        }

        private Page FindPageFor(long key)
        {
            var header = (FixedSizeTreeHeader.Large*)_parent.DirectRead(_treeName);
            var page = _tx.GetReadOnlyPage(header->RootPageNumber);
            if (_cursor == null)
                _cursor = new Stack<Page>();
            else
                _cursor.Clear();

            while (page.IsLeaf == false)
            {
                _cursor.Push(page);
                BinarySearch(page, key);
                if (page.LastMatch < 0 && page.LastSearchPosition > 0)
                    page.LastSearchPosition--;
                var childPageNumber = PageValueFor(page, page.LastSearchPosition);
                page = _tx.GetReadOnlyPage(childPageNumber);
            }

#if DEBUG
            if (page.LastMatch == 0 && _cursor.Count > 0)
            {
                var firstKey = KeyFor(page, 0);
                var parentPage = _cursor.Peek();
                var separatorKey = GetSeparatorKeyAtPosition(parentPage, parentPage.LastSearchPosition)[0];
                if (separatorKey != firstKey && (separatorKey != long.MinValue || parentPage.LastSearchPosition != 0))
                {
                    throw new InvalidOperationException(string.Format("Separator key ({0}) must be the same as the first key ({1}) in the page", separatorKey, firstKey));
                }
            }
#endif

            BinarySearch(page, key);
            return page;
        }

        private Page PageSplit(Page page, long key)
        {
            var largePtr = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
            Page parentPage = _cursor.Count > 0 ? _cursor.Pop() : null;
            if (parentPage == null) // root split
            {
                parentPage = _parent.NewPage(PageFlags.Branch | PageFlags.FixedSize, 1);
                parentPage.FixedSize_NumberOfEntries = 1;
                parentPage.FixedSize_StartPosition = (ushort)Constants.PageHeaderSize;
                parentPage.FixedSize_ValueSize = _valSize;
                
                largePtr->RootPageNumber = parentPage.PageNumber;
                largePtr->Depth++;
                var dataStart = GetSeparatorKeyAtPosition(parentPage);
                dataStart[0] = long.MinValue;
                dataStart[1] = page.PageNumber;
            }

            parentPage = _tx.ModifyPage(parentPage.PageNumber, _parent, parentPage);

            if (page.IsLeaf) // simple case of splitting a leaf pageNum
            {
                var newPage = _parent.NewPage(PageFlags.Leaf | PageFlags.FixedSize, 1);
                newPage.FixedSize_StartPosition = (ushort)Constants.PageHeaderSize;
                newPage.FixedSize_ValueSize = _valSize;
                newPage.FixedSize_NumberOfEntries = 0;

                // need to add past end of pageNum, optimized
                if (page.LastSearchPosition >= page.FixedSize_NumberOfEntries)
                {
                    AddLeafKey(newPage, 0, key);

                    AddSeparatorToParentPage(parentPage, parentPage.LastSearchPosition + 1, key, newPage.PageNumber);

                    largePtr->NumberOfEntries++;
                }
                else // not at end, random inserts, split page 3/4 to 1/4
                {
                    var entriesToMove = (ushort)(page.FixedSize_NumberOfEntries / 4);
                    newPage.FixedSize_NumberOfEntries = entriesToMove;
                    page.FixedSize_NumberOfEntries -= entriesToMove;
                    Memory.Copy(newPage.Base + newPage.FixedSize_StartPosition,
                        page.Base + page.FixedSize_StartPosition + (page.FixedSize_NumberOfEntries * _entrySize),
                        newPage.FixedSize_NumberOfEntries * _entrySize
                        );
                    AddSeparatorToParentPage(parentPage, parentPage.LastSearchPosition + 1, KeyFor(newPage,0), newPage.PageNumber);
                }
                return null;// we don't care about it for leaf pages
            }
            else // branch page
            {
                var newPage = _parent.NewPage(PageFlags.Branch | PageFlags.FixedSize, 1);
                newPage.FixedSize_StartPosition = (ushort)Constants.PageHeaderSize;
                newPage.FixedSize_ValueSize = _valSize;
                newPage.FixedSize_NumberOfEntries = 0;
                if (page.LastMatch > 0)
                    page.LastSearchPosition++;
                // need to add past end of pageNum, optimized
                if (page.LastSearchPosition >= page.FixedSize_NumberOfEntries)
                {
                    // here we steal the last entry from the current page so we maintain the implicit null left entry

                    var dataStart = GetSeparatorKeyAtPosition(newPage);
                    dataStart[0] = KeyFor(page, page.FixedSize_NumberOfEntries - 1);
                    dataStart[1] = PageValueFor(page, page.FixedSize_NumberOfEntries - 1);

                    newPage.FixedSize_NumberOfEntries++;
                    page.FixedSize_NumberOfEntries--;

                    AddSeparatorToParentPage(parentPage, parentPage.LastSearchPosition + 1, dataStart[0],
                        newPage.PageNumber);

                    return newPage; // this is where the new entry needs to go
                }
                // not at end, random inserts, split page 3/4 to 1/4

                var entriesToMove = (ushort)(page.FixedSize_NumberOfEntries / 4);
                newPage.FixedSize_NumberOfEntries = entriesToMove;
                page.FixedSize_NumberOfEntries -= entriesToMove;
                Memory.Copy(newPage.Base + newPage.FixedSize_StartPosition,
                    page.Base + page.FixedSize_StartPosition + (page.FixedSize_NumberOfEntries * BranchEntrySize),
                    newPage.FixedSize_NumberOfEntries * BranchEntrySize
                    );

                var newKey = KeyFor(newPage, 0);

                AddSeparatorToParentPage(parentPage, parentPage.LastSearchPosition + 1, newKey, newPage.PageNumber);

                return (newKey > key) ? page : newPage;
            }
        }

        private void AddLeafKey(Page page, int position, long key)
        {
            SetSeparatorKeyAtPosition(page, key, position);
            page.FixedSize_NumberOfEntries++;
        }

        private void AddSeparatorToParentPage(Page parentPage, int position, long key, long pageNum)
        {
            if ((parentPage.FixedSize_NumberOfEntries + 1) * BranchEntrySize > parentPage.PageMaxSpace)
            {
                parentPage = PageSplit(parentPage, key);
                System.Diagnostics.Debug.Assert(parentPage != null);
                BinarySearch(parentPage, key);
                position = parentPage.LastSearchPosition;
                if (parentPage.LastMatch > 0)
                    position++;
            }

            var entriesToMove = parentPage.FixedSize_NumberOfEntries - (position);
            var newEntryPos = parentPage.Base + parentPage.FixedSize_StartPosition + (position * BranchEntrySize);
            if (entriesToMove > 0)
            {
                UnmanagedMemory.Move(newEntryPos + BranchEntrySize,
                    newEntryPos,
                    entriesToMove * BranchEntrySize);
            }
            parentPage.FixedSize_NumberOfEntries++;
            ((long*)newEntryPos)[0] = key;
            ((long*)newEntryPos)[1] = pageNum;
        }

        private byte* AddEmbeddedEntry(long key, out bool isNew)
        {
            var ptr = _parent.DirectRead(_treeName);
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
            var newSize = (newEntriesCount * _entrySize);
            TemporaryPage tmp;
            using (_tx.Environment.GetTemporaryPage(_tx, out tmp))
            {
                int srcCopyStart = pos * _entrySize;
                Memory.Copy(tmp.TempPagePointer, dataStart, srcCopyStart);
                var newEntryStart = tmp.TempPagePointer + srcCopyStart;
                *((long*)newEntryStart) = key;

                Memory.Copy(newEntryStart + _entrySize, dataStart + srcCopyStart, (startingEntryCount - pos) * _entrySize);

                if (newEntriesCount > _maxEmbeddedEntries)
                {
                    // convert to large database
                    _flags = FixedSizeTreeHeader.OptionFlags.Large;
                    var allocatePage = _parent.NewPage(PageFlags.Leaf, 1);

                    var largeHeader = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
                    largeHeader->NumberOfEntries = newEntriesCount;
                    largeHeader->ValueSize = _valSize;
                    largeHeader->Depth = 1;
                    largeHeader->Flags = FixedSizeTreeHeader.OptionFlags.Large;
                    largeHeader->RootPageNumber = allocatePage.PageNumber;

                    allocatePage.Flags = PageFlags.FixedSize | PageFlags.Leaf;
                    allocatePage.PageNumber = allocatePage.PageNumber;
                    allocatePage.FixedSize_NumberOfEntries = newEntriesCount;
                    allocatePage.FixedSize_ValueSize = _valSize;
                    allocatePage.FixedSize_StartPosition = (ushort)Constants.PageHeaderSize;
                    Memory.Copy(allocatePage.Base + allocatePage.FixedSize_StartPosition, tmp.TempPagePointer,
                        newSize);

                    return allocatePage.Base + allocatePage.FixedSize_StartPosition + srcCopyStart + sizeof(long);
                }
                else
                {
                    byte* newData = _parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Embedded) + newSize);
                    header = (FixedSizeTreeHeader.Embedded*)newData;
                    header->ValueSize = _valSize;
                    header->Flags = FixedSizeTreeHeader.OptionFlags.Embedded;
                    header->NumberOfEntries = newEntriesCount;

                    Memory.Copy(newData + sizeof(FixedSizeTreeHeader.Embedded), tmp.TempPagePointer,
                        newSize);

                    return newData + sizeof(FixedSizeTreeHeader.Embedded) + srcCopyStart + sizeof(long);
                }
            }
        }

        private byte* AddNewEntry(long key)
        {
            // new, just create it & go
            var ptr = _parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Embedded) + _entrySize);
            var header = (FixedSizeTreeHeader.Embedded*)ptr;
            header->Flags = FixedSizeTreeHeader.OptionFlags.Embedded;
            header->ValueSize = _valSize;
            header->NumberOfEntries = 1;
            _flags = FixedSizeTreeHeader.OptionFlags.Embedded;

            byte* dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
            *(long*)(dataStart) = key;
            return (dataStart + sizeof(long));
        }

        private void BinarySearch(Page page, long val)
        {
            page.LastSearchPosition = BinarySearch(page.Base + page.FixedSize_StartPosition,
                page.FixedSize_NumberOfEntries, val,
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
        private long KeyFor(Page page, int num)
        {
            return KeyFor(page.Base + page.FixedSize_StartPosition, num, page.IsLeaf ? _entrySize : BranchEntrySize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long KeyFor(byte* p, int num, int size)
        {
            var lp = (long*)(p + (num * size));
            return lp[0];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long PageValueFor(Page page, int num)
        {
            var lp = (long*)((page.Base + page.FixedSize_StartPosition) + (num * (BranchEntrySize)) + sizeof(long));
            return lp[0];
        }

        public List<long> AllPages()
        {
            var results = new List<long>();
            switch (_flags)
            {
                case null:
                    break;
                case FixedSizeTreeHeader.OptionFlags.Embedded:
                    break;
                case FixedSizeTreeHeader.OptionFlags.Large:
                    var largePtr = (FixedSizeTreeHeader.Large*)_parent.DirectRead(_treeName);
                    var root = _tx.GetReadOnlyPage(largePtr->RootPageNumber);

                    var stack = new Stack<Page>();
                    stack.Push(root);

                    while (stack.Count > 0)
                    {
                        var p = stack.Pop();
                        results.Add(p.PageNumber);

                        if (p.IsBranch)
                        {
                            for (int j = 0; j < p.FixedSize_NumberOfEntries; j++)
                            {
                                var chhildNumber = PageValueFor(p, j);
                                stack.Push(_tx.GetReadOnlyPage(chhildNumber));
                            }
                        }
                    }

                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return results;
        }

        public bool Contains(long key)
        {
            byte* dataStart;
            switch (_flags)
            {
                case null:
                    return false;
                case FixedSizeTreeHeader.OptionFlags.Embedded:
                    var embeddedPtr = _parent.DirectRead(_treeName);
                    var header = (FixedSizeTreeHeader.Embedded*)embeddedPtr;
                    dataStart = embeddedPtr + sizeof(FixedSizeTreeHeader.Embedded);
                    BinarySearch(dataStart, header->NumberOfEntries, key, _entrySize);
                    return _lastMatch == 0;
                case FixedSizeTreeHeader.OptionFlags.Large:
                    var largePtr = (FixedSizeTreeHeader.Large*)_parent.DirectRead(_treeName);
                    var page = _tx.GetReadOnlyPage(largePtr->RootPageNumber);

                    while (page.IsLeaf == false)
                    {
                        BinarySearch(page, key);
                        if (page.LastMatch < 0 && page.LastSearchPosition > 0)
                            page.LastSearchPosition--;
                        var childPageNumber = PageValueFor(page, page.LastSearchPosition);
                        page = _tx.GetReadOnlyPage(childPageNumber);
                    }
                    dataStart = page.Base + page.FixedSize_StartPosition;

                    BinarySearch(dataStart, page.FixedSize_NumberOfEntries, key, _entrySize);
                    return _lastMatch == 0;
            }

            return false;
        }

        public DeletionResult Delete(long key)
        {
            switch (_flags)
            {
                case null:
                    // nothing to do
                    return new DeletionResult();
                case FixedSizeTreeHeader.OptionFlags.Embedded:
                    return RemoveEmbeddedEntry(key);
                case FixedSizeTreeHeader.OptionFlags.Large:
                    return RemoveLargeEntry(key);
            }

            throw new InvalidOperationException("Flags has invalid value: " + _flags);
        }

        public struct DeletionResult
        {
            public long NumberOfEntriesDeleted;
            public bool TreeRemoved;
        }

        public DeletionResult DeleteRange(long start, long end)
        {
            if (start > end)
                throw new InvalidOperationException("Start range cannot be greater than the end of the range");

            long entriedDeleted;
            switch (_flags)
            {
                case null:
                    entriedDeleted = 0;
                    break;
                case FixedSizeTreeHeader.OptionFlags.Embedded:
                    entriedDeleted = DeleteRangeEmbedded(start, end);
                    break;
                case FixedSizeTreeHeader.OptionFlags.Large:
                    entriedDeleted = DeleteRangeLarge(start, end);
                    break;
                default:
                    throw new InvalidOperationException("Flags value is not valid: " + _flags);
            }
            return new DeletionResult
            {
                NumberOfEntriesDeleted = entriedDeleted,
                TreeRemoved = _flags == null
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
                _flags = null;
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
            header->Flags = FixedSizeTreeHeader.OptionFlags.Embedded;

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
            Page page;
            var largeHeader = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
            while (true)
            {
                page = FindPageFor(start);
                if (page.LastMatch > 0)
                    page.LastSearchPosition++;
                if (page.LastSearchPosition < page.FixedSize_NumberOfEntries)
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
                var lastKey = KeyFor(nextPage, nextPage.FixedSize_NumberOfEntries - 1);
                if (lastKey >= end)
                    break; // we can't delete the entire page, special case handling follows

                entriesDeleted += nextPage.FixedSize_NumberOfEntries;
                largeHeader->NumberOfEntries -= nextPage.FixedSize_NumberOfEntries;


                var treeDeleted = RemoveEntirePage(nextPage, largeHeader); // this will rebalance the tree if needed
                System.Diagnostics.Debug.Assert(treeDeleted == false);
            }

            // we now know that the tree contains a maximum of 2 pages with the range
            // now remove the start range from the start page, we do this twice to cover the case 
            // where the start & end are on separate pages
            int rangeRemoved = 1;
            while (rangeRemoved > 0 &&
                _flags == FixedSizeTreeHeader.OptionFlags.Large // we may revert to embedded by the deletions, or remove entirely
                )
            {
                page = FindPageFor(start);
                if (page.LastMatch > 0)
                    page.LastSearchPosition++;
                if (page.LastSearchPosition < page.FixedSize_NumberOfEntries)
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
                if (_flags == FixedSizeTreeHeader.OptionFlags.Large)// we might have converted to embedded, in which case we can't use it
                    largeHeader->NumberOfEntries -= rangeRemoved;
                entriesDeleted += rangeRemoved;
            }
            if (_flags == FixedSizeTreeHeader.OptionFlags.Embedded)
            {
                // we converted to embeded in the delete, but might still have some range there
                return entriesDeleted + DeleteRangeEmbedded(start, end);
            }
            // note that because we call RebalancePage from RemoveRangeFromPage
            return entriesDeleted;
        }

        private Page GetNextLeafPage()
        {
            while (_cursor.Count > 0)
            {
                var page = _cursor.Peek();
                if (++page.LastSearchPosition >= page.FixedSize_NumberOfEntries)
                {
                    _cursor.Pop();
                    continue;
                }

                var nextPageNum = PageValueFor(page, page.LastSearchPosition);
                var childPage = _tx.GetReadOnlyPage(nextPageNum);
                if (childPage.IsLeaf)
                    return childPage;
                _cursor.Push(childPage);
            }
            return null;
        }

        private int RemoveRangeFromPage(Page page, long rangeEnd, FixedSizeTreeHeader.Large* largeHeader)
        {
            var startPos = page.LastSearchPosition;
            BinarySearch(page, rangeEnd);
            var endPos = page.LastSearchPosition;
            if (page.LastMatch < 0)
                endPos--;
            if (endPos == -1)
            {
                return 0;
            }
            if (startPos == endPos)
            {
                var key = KeyFor(page, startPos);
                if (key > rangeEnd)
                    return 0;
            }

            page = _tx.ModifyPage(page.PageNumber, _parent, page);
            var entriesDeleted = (endPos - startPos + 1);
            if (startPos == 0)
            {
                // if this is the very first item in the page, we can just change the start position
                page.FixedSize_StartPosition += (ushort)(_entrySize * entriesDeleted);
            }
            else
            {
                UnmanagedMemory.Move(page.Base + page.FixedSize_StartPosition + (startPos*_entrySize),
                    page.Base + page.FixedSize_StartPosition + ((endPos + 1)*_entrySize),
                    ((page.FixedSize_NumberOfEntries - endPos - 1)*_entrySize)
                    );
            }
            page.FixedSize_NumberOfEntries -= (ushort)entriesDeleted;

            if (page.FixedSize_NumberOfEntries == 0)
            {
                if (RemoveEntirePage(page, largeHeader))
                    return entriesDeleted;
            }
            else
            {
                if (startPos == 0 && _cursor.Count > 0)
                {
                    var parentPage = _cursor.Pop();
                    var separatorKey = KeyFor(page, 0);
                    if (parentPage.LastSearchPosition > 0 || separatorKey != long.MinValue)
                    {
                        parentPage = _tx.ModifyPage(parentPage.PageNumber, _parent, parentPage);
                        SetSeparatorKeyAtPosition(parentPage, separatorKey, parentPage.LastSearchPosition);
                    }
                    _cursor.Push(parentPage);
                }

                while (page != null)
                {
                    page = RebalancePage(page, largeHeader);
                }
            }
            return entriesDeleted;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long* GetSeparatorKeyAtPosition(Page page, int position = 0)
        {
            var dataStart = (long*)(page.Base + page.FixedSize_StartPosition + (BranchEntrySize * position));
            return dataStart;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long* SetSeparatorKeyAtPosition(Page page, long key, int position = 0)
        {
            var dataStart = GetSeparatorKeyAtPosition(page, position);
            dataStart[0] = key;
            return dataStart;
        }

        private bool RemoveEntirePage(Page page, FixedSizeTreeHeader.Large* largeHeader)
        {
            _tx.FreePage(page.PageNumber);
            if (_cursor.Count == 0) //remove the root page
            {
                _parent.Delete(_treeName);
                _flags = null;
                return true;
            }
            var parentPage = _cursor.Pop();
            parentPage = _tx.ModifyPage(parentPage.PageNumber, _parent, parentPage);
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
            page = _tx.ModifyPage(page.PageNumber, _parent, page);

            var largeHeader = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
            largeHeader->NumberOfEntries--;

            RemoveEntryFromPage(page, page.LastSearchPosition);

            while (page != null)
            {
                page = RebalancePage(page, largeHeader);
            }

            return new DeletionResult {NumberOfEntriesDeleted = 1};
        }

        private void RemoveEntryFromPage(Page page, int pos)
        {
            page.FixedSize_NumberOfEntries--;
            var size = (ushort)(page.IsLeaf ? _entrySize : BranchEntrySize);
            if (pos == 0)
            {
                // optimized, just move the start position
                page.FixedSize_StartPosition += size;
                return;
            }
            // have to move the memory
            UnmanagedMemory.Move(page.Base + page.FixedSize_StartPosition + (pos * size),
                   page.Base + page.FixedSize_StartPosition + ((pos + 1) * size),
                   (page.FixedSize_NumberOfEntries - pos + 1) * size);
        }

        private Page RebalancePage(Page page, FixedSizeTreeHeader.Large* largeTreeHeader)
        {
            if (_cursor.Count == 0)
            {
                // root page
                if (page.FixedSize_NumberOfEntries <= _maxEmbeddedEntries && page.IsLeaf)
                {
                    // and small enough to fit, converting to embedded
                    var ptr = _parent.DirectAdd(_treeName,
                        sizeof(FixedSizeTreeHeader.Embedded) + (_entrySize * page.FixedSize_NumberOfEntries));
                    var header = (FixedSizeTreeHeader.Embedded*)ptr;
                    header->Flags = FixedSizeTreeHeader.OptionFlags.Embedded;
                    header->ValueSize = _valSize;
                    header->NumberOfEntries = (byte)page.FixedSize_NumberOfEntries;
                    _flags = FixedSizeTreeHeader.OptionFlags.Embedded;

                    Memory.Copy(ptr + sizeof(FixedSizeTreeHeader.Embedded),
                        page.Base + page.FixedSize_StartPosition,
                        (_entrySize * page.FixedSize_NumberOfEntries));

                    _tx.FreePage(page.PageNumber);
                }
                if (page.IsBranch && page.FixedSize_NumberOfEntries == 1)
                {
                    var childPage = PageValueFor(page, 0);
                    var rootPageNum = page.PageNumber;
                    Memory.Copy(page.Base, _tx.GetReadOnlyPage(childPage).Base, AbstractPager.PageSize);
                    page.PageNumber = rootPageNum;//overwritten by copy

                    if (largeTreeHeader != null)
                        largeTreeHeader->Depth--;

                    _tx.FreePage(childPage);
                }

                return null;
            }


            var sizeOfEntryInPage = (page.IsLeaf ? _entrySize : BranchEntrySize);
            var minNumberOfEntriesBeforeRebalance = (AbstractPager.PageMaxSpace / sizeOfEntryInPage) / 4;
            if (page.FixedSize_NumberOfEntries > minNumberOfEntriesBeforeRebalance)
            {
                // if we have more than 25% of the entries that would fit in the page, there is nothing that needs to be done
                // so we are done
                return null;
            }

            // we determined that we require rebalancing...

            var parentPage = _cursor.Pop();
            parentPage = _tx.ModifyPage(parentPage.PageNumber, _parent, parentPage);

            if (page.FixedSize_NumberOfEntries == 0)// empty page, delete it and fixup the parent
            {
                // fixup the implicit less than ref
                if (parentPage.LastSearchPosition == 0 && parentPage.NumberOfEntries > 2)
                {
                    parentPage.FixedSize_NumberOfEntries--;
                    // remove the first value
                    parentPage.FixedSize_StartPosition += BranchEntrySize;
                    // set the next value (now the first), to be smaller than everything
                    SetSeparatorKeyAtPosition(parentPage, long.MinValue);
                }
                else
                {
                    // need to remove from midway through. At any rate, we'll rebalance on next call
                    RemoveEntryFromPage(parentPage, parentPage.LastSearchPosition);
                }
                return parentPage;
            }

            if (page.IsBranch && page.FixedSize_NumberOfEntries == 1)
            {
                // we can just collapse this to the parent
                // write the page value to the parent
                SetSeparatorKeyAtPosition(parentPage, PageValueFor(page, 0), parentPage.LastSearchPosition);
                // then delete the page
                _tx.FreePage(page.PageNumber);
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

            System.Diagnostics.Debug.Assert(parentPage.FixedSize_NumberOfEntries >= 2);//otherwise this isn't a valid branch page
            if (parentPage.LastSearchPosition == 0)
            {
                // the current page is the leftmost one, so let us try steal some data
                // from the one on the right
                var siblingNum = PageValueFor(parentPage, 1);
                var siblingPage = _tx.GetReadOnlyPage(siblingNum);
                if (siblingPage.Flags != page.Flags)
                    return null; // we cannot steal from a leaf sibling if we are branch, or vice versa

                if (siblingPage.FixedSize_NumberOfEntries <= minNumberOfEntriesBeforeRebalance * 2)
                {
                    // we can merge both pages into a single one and still have enough over
                    ResetStartPosition(page);
                    Memory.Copy(
                        page.Base + page.FixedSize_StartPosition + (page.FixedSize_NumberOfEntries * sizeOfEntryInPage),
                        siblingPage.Base + siblingPage.FixedSize_StartPosition,
                        siblingPage.FixedSize_NumberOfEntries * sizeOfEntryInPage
                        );
                    page.FixedSize_NumberOfEntries += siblingPage.FixedSize_NumberOfEntries;

                    _tx.FreePage(siblingNum);

                    // now fix parent ref, in this case, just removing it is enough
                    RemoveEntryFromPage(parentPage, 1);

                    return parentPage;
                }
                // too big to just merge, let just take half of the sibling and move on
                var entriesToTake = (siblingPage.FixedSize_NumberOfEntries / 2);
                ResetStartPosition(page);
                Memory.Copy(
                    page.Base + page.FixedSize_StartPosition + (page.FixedSize_NumberOfEntries * sizeOfEntryInPage),
                    siblingPage.Base + siblingPage.FixedSize_StartPosition,
                    entriesToTake * sizeOfEntryInPage
                    );
                page.FixedSize_NumberOfEntries += (ushort)entriesToTake;
                siblingPage.FixedSize_NumberOfEntries -= (ushort)entriesToTake;
                siblingPage.FixedSize_StartPosition += (ushort)(sizeOfEntryInPage * entriesToTake);

                // now update the new separator in the sibling position in the parent
                var newSeparator = KeyFor(siblingPage, 0);
                SetSeparatorKeyAtPosition(parentPage, newSeparator, 1);

                return parentPage;
            }
            else // we aren't the leftmost item, so we will take from the page on our left
            {
                var siblingNum = PageValueFor(parentPage, parentPage.LastSearchPosition - 1);
                var siblingPage = _tx.GetReadOnlyPage(siblingNum);
                siblingPage = _tx.ModifyPage(siblingPage.PageNumber, _parent, siblingPage);
                if (siblingPage.Flags != page.Flags)
                    return null; // we cannot steal from a leaf sibling if we are branch, or vice versa

                if (siblingPage.FixedSize_NumberOfEntries <= minNumberOfEntriesBeforeRebalance * 2)
                {
                    // we can merge both pages into a single one and still have enough over
                    ResetStartPosition(siblingPage);
                    Memory.Copy(
                        siblingPage.Base + siblingPage.FixedSize_StartPosition + (siblingPage.FixedSize_NumberOfEntries * sizeOfEntryInPage),
                        page.Base + page.FixedSize_StartPosition,
                        page.FixedSize_NumberOfEntries * sizeOfEntryInPage
                        );
                    siblingPage.FixedSize_NumberOfEntries += page.FixedSize_NumberOfEntries;

                    _tx.FreePage(page.PageNumber);

                    // now fix parent ref, in this case, just removing it is enough
                    RemoveEntryFromPage(parentPage, parentPage.LastSearchPosition);

                    return parentPage;
                }
                // too big to just merge, let just take half of the sibling and move on
                var entriesToTake = (siblingPage.FixedSize_NumberOfEntries / 2);
                ResetStartPosition(page);
                UnmanagedMemory.Move(page.Base + page.FixedSize_StartPosition + (entriesToTake * sizeOfEntryInPage),
                    page.Base + page.FixedSize_StartPosition,
                    entriesToTake * sizeOfEntryInPage);

                Memory.Copy(
                    page.Base + page.FixedSize_StartPosition,
                    siblingPage.Base + siblingPage.FixedSize_StartPosition + ((siblingPage.FixedSize_NumberOfEntries - entriesToTake) * sizeOfEntryInPage),
                    entriesToTake * sizeOfEntryInPage
                    );
                page.FixedSize_NumberOfEntries += (ushort)entriesToTake;
                siblingPage.FixedSize_NumberOfEntries -= (ushort)entriesToTake;

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
                _flags = null;
                _parent.Delete(_treeName);
                return new DeletionResult {NumberOfEntriesDeleted = 1, TreeRemoved = true};
            }

            byte* newData = _parent.DirectAdd(_treeName,
                sizeof(FixedSizeTreeHeader.Embedded) + ((startingEntryCount - 1) * _entrySize));

            int srcCopyStart = pos * _entrySize + sizeof(FixedSizeTreeHeader.Embedded);
            Memory.Copy(newData, ptr, srcCopyStart);
            Memory.Copy(newData + srcCopyStart, ptr + srcCopyStart + _entrySize, (header->NumberOfEntries - pos) * _entrySize);

            header = (FixedSizeTreeHeader.Embedded*)newData;
            header->NumberOfEntries--;
            header->ValueSize = _valSize;
            header->Flags = FixedSizeTreeHeader.OptionFlags.Embedded;
            return new DeletionResult {NumberOfEntriesDeleted = 1};
        }

        public Slice Read(long key)
        {
            switch (_flags)
            {
                case null:
                    return null;
                case FixedSizeTreeHeader.OptionFlags.Embedded:
                    var ptr = _parent.DirectRead(_treeName);
                    var header = (FixedSizeTreeHeader.Embedded*)ptr;
                    var dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
                    var pos = BinarySearch(dataStart, header->NumberOfEntries, key, _entrySize);
                    if (_lastMatch != 0)
                        return null;
                    return new Slice(dataStart + (pos * _entrySize) + sizeof(long), _valSize);
                case FixedSizeTreeHeader.OptionFlags.Large:
                    var largePtr = (FixedSizeTreeHeader.Large*)_parent.DirectRead(_treeName);
                    var page = _tx.GetReadOnlyPage(largePtr->RootPageNumber);

                    while (page.IsLeaf == false)
                    {
                        BinarySearch(page, key);
                        if (page.LastMatch < 0 && page.LastSearchPosition > 0)
                            page.LastSearchPosition--;
                        var childPageNumber = PageValueFor(page, page.LastSearchPosition);
                        page = _tx.GetReadOnlyPage(childPageNumber);
                    }
                    dataStart = page.Base + page.FixedSize_StartPosition;

                    BinarySearch(page, key);
                    if (_lastMatch != 0)
                        return null;
                    return new Slice(dataStart + (page.LastSearchPosition * _entrySize) + sizeof(long), _valSize);
            }

            return null;
        }

        public IFixedSizeIterator Iterate()
        {
            switch (_flags)
            {
                case null:
                    return new NullIterator();
                case FixedSizeTreeHeader.OptionFlags.Embedded:
                    return new EmbeddedIterator(this);
                case FixedSizeTreeHeader.OptionFlags.Large:
                    return new LargeIterator(this);
            }

            return null;
        }

        public long NumberOfEntries
        {
            get
            {
                var header = _parent.DirectRead(_treeName);
                if (header == null)
                    return 0;

                var flags = ((FixedSizeTreeHeader.Embedded*) header)->Flags;
                switch (flags)
                {
                    case FixedSizeTreeHeader.OptionFlags.Embedded:
                        return ((FixedSizeTreeHeader.Embedded*)header)->NumberOfEntries;
                    case FixedSizeTreeHeader.OptionFlags.Large:
                        return ((FixedSizeTreeHeader.Large*)header)->NumberOfEntries;
                    default:
                        return 0;
                }
            }
        }

        [Conditional("DEBUG")]
        public void DebugRenderAndShow()
        {
            DebugStuff.RenderAndShow_FixedSizeTree(_tx, _parent, _treeName);
        }
    }
}
