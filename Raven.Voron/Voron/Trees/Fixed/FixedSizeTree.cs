﻿// -----------------------------------------------------------------------
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
            return Debug(p.Base + p.FixedSize_StartPosition, p.FixedSize_NumberOfEntries,
                p.IsLeaf ? _entrySize : BranchEntrySize);
        }

        public Slice Name
        {
            get { return _treeName; }
        }

        public long[] Debug(byte* p, int entries, int size)
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
            if (_valSize == 0 && val != null)
                throw new InvalidOperationException("When the value size is zero, no value can be specified");
            if (_valSize != 0 && val == null)
                throw new InvalidOperationException("When the value size is not zero, the value must be specified");
            if (val != null && val.Size != _valSize)
                throw new InvalidOperationException("The value size must be " + _valSize + " but was " + val.Size);

	        bool isNew;
	        var pos = DirectAdd(key, out isNew);
            if (val != null)
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
            var headerToWrite = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
            headerToWrite->NumberOfEntries++;

            if (page.LastMatch > 0)
                page.LastSearchPosition++; // after the last one

            if ((page.FixedSize_NumberOfEntries + 1) * _entrySize > page.PageMaxSpace)
            {
                PageSplit(page, key);

                // now we know we have enough space, or we need to split the parent page
	            var addLargeEntry = AddLargeEntry(key, out isNew);
				isNew = true;
				return addLargeEntry;
            }

            if (page.FixedSize_StartPosition != Constants.PageHeaderSize)
            {
                // we need to move it back, then add the new item
                UnmanagedMemory.Move(page.Base + Constants.PageHeaderSize,
                    page.Base + page.FixedSize_StartPosition,
                    page.FixedSize_NumberOfEntries * _entrySize);
                page.FixedSize_StartPosition = (ushort)Constants.PageHeaderSize;
            }

            var entriesToMove = page.FixedSize_NumberOfEntries - page.LastSearchPosition;
            if (entriesToMove > 0)
            {
				UnmanagedMemory.Move(page.Base + page.FixedSize_StartPosition + ((page.LastSearchPosition + 1) * _entrySize),
                    page.Base + page.FixedSize_StartPosition + (page.LastSearchPosition * _entrySize),
                    entriesToMove * _entrySize);
            }
            page.FixedSize_NumberOfEntries++;
	        isNew = true;
            *((long*)(page.Base + page.FixedSize_StartPosition + (page.LastSearchPosition * _entrySize))) = key;
            return (page.Base + page.FixedSize_StartPosition + (page.LastSearchPosition * _entrySize) + sizeof(long));
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
                BinarySearch(page, key, BranchEntrySize);
                if (page.LastMatch < 0 && page.LastSearchPosition > 0)
                    page.LastSearchPosition--;
                var childPageNumber = PageValueFor(page.Base + page.FixedSize_StartPosition, page.LastSearchPosition);
                page = _tx.GetReadOnlyPage(childPageNumber);
            }

            BinarySearch(page, key, _entrySize);
            return page;
        }

        private void PageSplit(Page page, long key)
        {
            Page parentPage = _cursor.Count > 0 ? _cursor.Pop() : null;
            if (parentPage != null)
            {
                if ((parentPage.FixedSize_NumberOfEntries + 1) * BranchEntrySize > parentPage.PageMaxSpace)
                {
                    if (parentPage.LastMatch > 0)
                        parentPage.LastSearchPosition++; // after the last one
                    PageSplit(parentPage, key);
                    return;
                }
            }

            if (page.FixedSize_StartPosition != Constants.PageHeaderSize)
            {
				UnmanagedMemory.Move(page.Base + Constants.PageHeaderSize,
                    page.Base + page.FixedSize_StartPosition,
                    page.FixedSize_NumberOfEntries * (page.IsLeaf ? _entrySize : BranchEntrySize));
                page.FixedSize_StartPosition = (ushort)Constants.PageHeaderSize;
            }

            long splitKey;
            long splitPageNumber;
            ActuallySplitPage(page, key, out splitKey, out splitPageNumber);

            if (parentPage == null) //root
            {
                var newRootPage = _parent.NewPage(PageFlags.Branch | PageFlags.FixedSize, 1);
                newRootPage.FixedSize_ValueSize = _valSize;
                newRootPage.FixedSize_StartPosition = (ushort)Constants.PageHeaderSize;

                var largePtr = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
                largePtr->RootPageNumber = newRootPage.PageNumber;

                newRootPage.FixedSize_NumberOfEntries = 2;
                var dataStart = (long*)(newRootPage.Base + newRootPage.FixedSize_StartPosition);
                dataStart[0] = long.MinValue;
                dataStart[1] = page.PageNumber;
                dataStart[2] = splitKey;
                dataStart[3] = splitPageNumber;
                return;
            }

            parentPage = _tx.ModifyPage(parentPage.PageNumber, _parent, parentPage);

            var entriesToMove = parentPage.FixedSize_NumberOfEntries - (parentPage.LastSearchPosition + 1);
            var newEntryPos = parentPage.Base + parentPage.FixedSize_StartPosition + ((parentPage.LastSearchPosition + 1) * BranchEntrySize);
            if (entriesToMove > 0)
            {
				UnmanagedMemory.Move(newEntryPos + BranchEntrySize,
                    newEntryPos,
                    entriesToMove * BranchEntrySize);
            }
            parentPage.FixedSize_NumberOfEntries++;
            ((long*)newEntryPos)[0] = splitKey;
            ((long*)newEntryPos)[1] = splitPageNumber;
        }

        private void ActuallySplitPage(Page page, long key, out long splitKey, out long splitPageNumber)
        {
            var rightPage = _parent.NewPage(PageFlags.Leaf | PageFlags.FixedSize, 1);
            rightPage.FixedSize_ValueSize = _valSize;
            rightPage.FixedSize_StartPosition = (ushort)Constants.PageHeaderSize;
            splitPageNumber = rightPage.PageNumber;
            // if we are at the end, just create a new one
            if (page.LastSearchPosition == page.FixedSize_NumberOfEntries)
            {
                rightPage.FixedSize_NumberOfEntries = 1;
                splitKey = key;
                *((long*)(rightPage.Base + rightPage.FixedSize_StartPosition)) = key;
            }
            else // need to actually split it, we'll move 1/4 of the entries to the right hand side
            {
                rightPage.FixedSize_NumberOfEntries = (ushort)(page.FixedSize_NumberOfEntries / 4);
                page.FixedSize_NumberOfEntries -= rightPage.FixedSize_NumberOfEntries;
                Memory.Copy(rightPage.Base + rightPage.FixedSize_StartPosition,
                    page.Base + page.FixedSize_StartPosition + (page.FixedSize_NumberOfEntries * _entrySize),
                    rightPage.FixedSize_NumberOfEntries * _entrySize
                    );

                splitKey = ((long*)(rightPage.Base + rightPage.FixedSize_StartPosition))[0];
            }
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

        private void BinarySearch(Page page, long val, int size)
        {
            page.LastSearchPosition = BinarySearch(page.Base + page.FixedSize_StartPosition, page.FixedSize_NumberOfEntries, val, size);
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
		        _lastMatch = val.CompareTo(KeyFor(p, position, size));
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
        private long KeyFor(byte* p, int num, int size)
        {
            var lp = (long*)(p + (num * size));
            return lp[0];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long PageValueFor(byte* p, int num)
        {
            var lp = (long*)(p + (num * (BranchEntrySize)) + sizeof(long));
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
								var chhildNumber = PageValueFor(p.Base + p.FixedSize_StartPosition, j);
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
                        BinarySearch(page, key, BranchEntrySize);
                        if (page.LastMatch < 0 && page.LastSearchPosition > 0)
                            page.LastSearchPosition--;
                        var childPageNumber = PageValueFor(page.Base + page.FixedSize_StartPosition, page.LastSearchPosition);
                        page = _tx.GetReadOnlyPage(childPageNumber);
                    }
                    dataStart = page.Base + page.FixedSize_StartPosition;

                    BinarySearch(dataStart, page.FixedSize_NumberOfEntries, key, _entrySize);
                    return _lastMatch == 0;
            }

            return false;
        }

        public void Delete(long key)
        {
            switch (_flags)
            {
                case null:
                    // nothing to do
                    break;
                case FixedSizeTreeHeader.OptionFlags.Embedded:
                    RemoveEmbeddedEntry(key);
                    break;
                case FixedSizeTreeHeader.OptionFlags.Large:
		            RemoveLargeEntry(key);
                    break;
            }
        }

		public long DeleteRange(long start, long end, out bool allRemoved)
		{
			if (start > end)
				throw new InvalidOperationException("Start range cannot be greater than the end of the range");

			switch (_flags)
			{
				case null:
					allRemoved = false;
					return -1;
				case FixedSizeTreeHeader.OptionFlags.Embedded:
					return DeleteRangeEmbedded(start, end, out allRemoved);
				case FixedSizeTreeHeader.OptionFlags.Large:
					return DeleteRangeLarge(start, end, out allRemoved);
			}

			throw new InvalidOperationException("Flags value is not valid: " + _flags);
		}

		private long DeleteRangeEmbedded(long start, long end, out bool allRemoved)
		{
			allRemoved = false;

			byte* ptr = _parent.DirectRead(_treeName);
			var header = (FixedSizeTreeHeader.Embedded*)ptr;
			var startingEntryCount = header->NumberOfEntries;
			var startPos = BinarySearch(ptr + sizeof(FixedSizeTreeHeader.Embedded), startingEntryCount, start, _entrySize);
			if (_lastMatch > 0)
			{
				return 0; // Greater than the values we have, nothing to do
			}
			var endPos = BinarySearch(ptr + sizeof(FixedSizeTreeHeader.Embedded), startingEntryCount, end, _entrySize);
			if (_lastMatch < 0)
			{
				return 0; // End key is before all the values we have, nothing to do
			}

			byte entriesDeleted = (byte) (endPos - startPos + 1);

		    if (entriesDeleted == header->NumberOfEntries)
		    {
			    _parent.Delete(_treeName);
			    _flags = null;
			    allRemoved = true;
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

	    private long DeleteRangeLarge(long start, long end, out bool isAllRemoved)
		{
		    isAllRemoved = false;

		    var page = FindPageFor(start);
		    if (page.LastMatch > 0)
			    return 0;

			var startSearchPosition = page.LastSearchPosition;
		    long entriesDeleted = 0;
			BinarySearch(page, end, _entrySize);
#if DEBUG
			long i = 0;
#endif
			do
		    {
#if DEBUG
			    i++;
#endif
				entriesDeleted += DeleteEntiresInPage(page, startSearchPosition, page.LastSearchPosition + 1, out isAllRemoved);
				startSearchPosition = 0;
			    
				if (page.LastMatch == 0 ||
			        _cursor.Count == 0 // No more pages
				    )
			    {
				    return entriesDeleted;
			    }

			    var parentPage = _cursor.Pop();
			    if (parentPage.IsLeaf)
			    {
					page = parentPage;
					BinarySearch(page, end, _entrySize);
					continue;
			    }

			    if (parentPage.LastSearchPosition < parentPage.FixedSize_NumberOfEntries - 1)
			    {
					parentPage.LastSearchPosition++;
					_cursor.Push(parentPage);
			    }
			    else
			    {
				    if (_cursor.Count > 0)
					    continue;

				    if (parentPage.IsBranch)
					    return entriesDeleted;

					// TODO: Please test this, this wasn't been tested
					Debugger.Break();
					page = parentPage;
					BinarySearch(page, end, _entrySize);
					continue;
			    }

				var childParentNumber = PageValueFor(parentPage.Base + parentPage.FixedSize_StartPosition, parentPage.LastSearchPosition);
				page = _tx.GetReadOnlyPage(childParentNumber);
				while (page.IsBranch)
			    {
					// TODO: Please test this, this wasn't been tested
					Debugger.Break();
					_cursor.Push(page);
					childParentNumber = PageValueFor(page.Base + page.FixedSize_StartPosition, page.LastSearchPosition);
					page = _tx.GetReadOnlyPage(childParentNumber);
			    }
				BinarySearch(page, end, _entrySize);
		    } while (page.LastMatch >= 0);

			return entriesDeleted;
	    }

		private int DeleteEntiresInPage(Page page, int startPosition, int endPosition, out bool allEntriesDeleted)
	    {
			page = _tx.ModifyPage(page.PageNumber, _parent, page);
			var entriesDeleted = endPosition - startPosition;

			var largeHeader = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
			largeHeader->NumberOfEntries -= entriesDeleted;
			page.FixedSize_NumberOfEntries -= (ushort)entriesDeleted;

			if (page.FixedSize_NumberOfEntries == 0)
			{
				allEntriesDeleted = DeleteEntirePage(page);
				return entriesDeleted;
			}

			allEntriesDeleted = false;

			// Is root and small enough for embedded, convert
			if (_cursor.Count == 0)
			{
				if (page.FixedSize_NumberOfEntries <= _maxEmbeddedEntries)
				{
					DeleteRangeInLargeRootPageAndConvertToEmbedded(page, startPosition, page.LastSearchPosition + 1,
						page.FixedSize_NumberOfEntries + entriesDeleted - page.LastSearchPosition - 1);
					return entriesDeleted;
				}
			}

			if (page.LastSearchPosition == 0)
			{
				// if this is the very first item in the page, we can just change the start position
				page.FixedSize_StartPosition += (ushort)_entrySize;
			}
			else
			{
				DeletePartInsideThePage(page, startPosition, endPosition, page.FixedSize_NumberOfEntries + entriesDeleted - page.LastSearchPosition - 1, _entrySize);
				// deleted and we are done
			}

			return entriesDeleted;
	    }

		private void DeletePartInsideThePage(Page page, int startPosition, int endPosition, int copyCount, int size)
		{
			System.Diagnostics.Debug.Assert(endPosition > startPosition, "This is probably a bug.");
			System.Diagnostics.Debug.Assert(copyCount >= 0, "We cannot copy negative amount. This is probably a bug.");
			UnmanagedMemory.Move(page.Base + page.FixedSize_StartPosition + (startPosition * size),
				page.Base + page.FixedSize_StartPosition + (endPosition * size),
				size * copyCount);
		}

		private void DeleteRangeInLargeRootPageAndConvertToEmbedded(Page page, int startPosition, int endPosition, int copyCount)
	    {
			var ptr = _parent.DirectAdd(_treeName,
						sizeof(FixedSizeTreeHeader.Embedded) + (_entrySize * page.FixedSize_NumberOfEntries));
			var header = (FixedSizeTreeHeader.Embedded*)ptr;
			header->Flags = FixedSizeTreeHeader.OptionFlags.Embedded;
			header->ValueSize = _valSize;
			header->NumberOfEntries = (byte)page.FixedSize_NumberOfEntries;
			_flags = FixedSizeTreeHeader.OptionFlags.Embedded;

			Memory.Copy(ptr + sizeof(FixedSizeTreeHeader.Embedded),
				page.Base + page.FixedSize_StartPosition,
				(_entrySize * startPosition));

			Memory.Copy(ptr + sizeof(FixedSizeTreeHeader.Embedded) + (_entrySize * startPosition),
				page.Base + page.FixedSize_StartPosition + (_entrySize * endPosition),
				(_entrySize * copyCount));

			_parent.FreePage(page);
	    }

		/// <summary>
		/// 
		/// </summary>
		/// <returns>True means all items are deleted</returns>
	    private bool DeleteEntirePage(Page page)
	    {
			/*
				Implementation details:
					- removed entire page? Update parent
					- removed entire page? and is root, remove all and delete entry
			 */

			_parent.FreePage(page);

		    if (_cursor.Count == 0)
		    {
				// Root, remove all and delete entry
				_parent.Delete(_treeName);
				_flags = null;
				return true; // This is the root, the tree doesn't have any entires left
		    }

			// Update parent
		    var parentPage = _cursor.Pop();
			var parentPageNumber = parentPage.PageNumber;
		    parentPage = _tx.ModifyPage(parentPageNumber, _parent, parentPage);
		    parentPage.FixedSize_NumberOfEntries--;

			if (parentPage.LastSearchPosition == 0)
			{
				// if this is the very first item in the page, we can just change the start position
				parentPage.FixedSize_StartPosition += BranchEntrySize;
			}
			else
			{
				// branch pages must have at least 2 entries, so this is safe to do
				DeleteEntryInPage (parentPage, BranchEntrySize);
			}

			// it has only one entry, we can delete it and switch with the last child item
			if (parentPage.FixedSize_NumberOfEntries == 1)
			{
				var lastChildNumber = *(long*)(parentPage.Base + parentPage.FixedSize_StartPosition + sizeof(long));
				var childPage = _tx.GetReadOnlyPage(lastChildNumber);
				Memory.Copy(parentPage.Base, childPage.Base, parentPage.PageSize);
				parentPage.PageNumber = parentPageNumber; // overwritten in copy
				_parent.FreePage(childPage);
		    }
			_cursor.Push(parentPage);

			return false; // This is not the root, the tree has more entires
	    }

	    private void DeleteEntryInPage(Page parentPage, int size)
	    {
			DeletePartInsideThePage(parentPage, parentPage.LastSearchPosition, parentPage.LastSearchPosition + 1, parentPage.FixedSize_NumberOfEntries - parentPage.LastSearchPosition, size);
			parentPage.LastSearchPosition--;
		}

	    private void RemoveLargeEntry(long key)
        {
            var page = FindPageFor(key);
            if (page.LastMatch != 0)
                return;
            page = _tx.ModifyPage(page.PageNumber, _parent, page);

            var largeHeader = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
            largeHeader->NumberOfEntries--;
            page.FixedSize_NumberOfEntries--;

            if (_cursor.Count == 0)
            {
                // root page
                if (page.FixedSize_NumberOfEntries <= _maxEmbeddedEntries)
                {
					DeleteRangeInLargeRootPageAndConvertToEmbedded(page, page.LastSearchPosition, page.LastSearchPosition + 1, 
						page.FixedSize_NumberOfEntries - page.LastSearchPosition);
                    return;
                }
            }

            if (page.FixedSize_NumberOfEntries > 0)
            {
                if (page.LastSearchPosition == 0)
                {
                    // if this is the very first item in the page, we can just change the start position
                    page.FixedSize_StartPosition += (ushort)_entrySize;
                    return;
                }

	            DeleteEntryInPage(page, _entrySize);
                // deleted and we are done
                return;
            }

            // now we need to remove from parent
		    DeleteEntirePage(page);
        }

        private void RemoveEmbeddedEntry(long key)
        {
            byte* ptr = _parent.DirectRead(_treeName);
            var header = (FixedSizeTreeHeader.Embedded*)ptr;
            var startingEntryCount = header->NumberOfEntries;
            var pos = BinarySearch(ptr + sizeof(FixedSizeTreeHeader.Embedded), startingEntryCount, key, _entrySize);
            if (_lastMatch != 0)
            {
				return; // not here, nothing to do
            }
            if (startingEntryCount == 1)
            {
                // only single entry, just remove it
                _flags = null;
                _parent.Delete(_treeName);
				header->NumberOfEntries--;
				return;
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
                        BinarySearch(page, key, BranchEntrySize);
                        if (page.LastMatch < 0 && page.LastSearchPosition > 0)
                            page.LastSearchPosition--;
                        var childPageNumber = PageValueFor(page.Base + page.FixedSize_StartPosition, page.LastSearchPosition);
                        page = _tx.GetReadOnlyPage(childPageNumber);
                    }
                    dataStart = page.Base + page.FixedSize_StartPosition;

                    BinarySearch(page, key, _entrySize);
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

	    public long NumberOfEntries()
	    {
			// both large & embedded have same position for number of entries, we can use both
			// regardless of type
			var header = (FixedSizeTreeHeader.Embedded*)_parent.DirectRead(_treeName);
		    if (header == null)
			    return 0;
			return header->NumberOfEntries;
	    }
    }
}