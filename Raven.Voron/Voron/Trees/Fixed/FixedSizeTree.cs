// -----------------------------------------------------------------------
//  <copyright file="FixedSizeTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Trees.Fixed
{
	public unsafe partial class FixedSizeTree
	{
		private const int BranchEntrySize = sizeof(long) + sizeof(long);
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

		public long[] Debug(byte* p, int entries)
		{
			var a = new long[entries];
			for (int i = 0; i < entries; i++)
			{
				a[i] = *((long*)(p + (_entrySize * i)));
			}
			return a;
		}

		public void Add(long key, Slice val = null)
		{
			if (_valSize == 0 && val != null)
				throw new InvalidOperationException("When the value size is zero, no value can be specified");
			if (_valSize != 0 && val == null)
				throw new InvalidOperationException("When the value size is not zero, the value must be specified");
			if (val != null && val.Size != _valSize)
				throw new InvalidOperationException("The value size must be " + _valSize + " but was " + val.Size);

			switch (_flags)
			{
				case null:
					AddNewEntry(key, val);
					break;
				case FixedSizeTreeHeader.OptionFlags.Embedded:
					AddEmbeddedEntry(key, val);
					break;
				case FixedSizeTreeHeader.OptionFlags.Large:
					AddLargeEntry(key, val);
					break;
			}
		}

		private void AddLargeEntry(long key, Slice val)
		{
			var page = FindPageFor(key);

			page = _tx.ModifyPage(page.PageNumber, _parent, page);

			if (_lastMatch == 0) // update
			{
				if (val == null)
					return;
				val.CopyTo(page.Base + Constants.PageHeaderSize + (page.LastSearchPosition * _entrySize) + sizeof(long));
				return;
			}
			var headerToWrite = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
			headerToWrite->NumberOfEntries++;

			if (page.LastMatch > 0)
				page.LastSearchPosition++; // after the last one

			if ((page.FixedSize_NumberOfEntries + 1) * _entrySize > page.PageMaxSpace)
			{
				PageSplit(page, key);
				// now we know we have enough space, or we need to split the parent page
				AddLargeEntry(key, val);
				return;
			}

			var entriesToMove = page.FixedSize_NumberOfEntries - page.LastSearchPosition;
			if (entriesToMove > 0)
			{
				StdLib.memmove(page.Base + Constants.PageHeaderSize + ((page.LastSearchPosition + 1) * _entrySize),
					page.Base + Constants.PageHeaderSize + (page.LastSearchPosition * _entrySize),
					entriesToMove * _entrySize);
			}
			page.FixedSize_NumberOfEntries++;
			*((long*)(page.Base + Constants.PageHeaderSize + (page.LastSearchPosition * _entrySize))) = key;
			if (val != null)
				val.CopyTo((page.Base + Constants.PageHeaderSize + (page.LastSearchPosition * _entrySize) + sizeof(long)));
		}

		private Page FindPageFor(long key)
		{
			var header = (FixedSizeTreeHeader.Large*) _parent.DirectRead(_treeName);
			var page = _tx.GetReadOnlyPage(header->RootPageNumber);
			if (_cursor == null)
				_cursor = new Stack<Page>();
			else
				_cursor.Clear();

			while (page.IsLeaf == false)
			{
				_cursor.Push(page);
				BinarySearch(page, key, BranchEntrySize);
				var childPageNumber = PageValueFor(page.Base + Constants.PageHeaderSize, page.LastSearchPosition, BranchEntrySize);
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

			long splitKey;
			long splitPageNumber;
			ActuallySplitPage(page, key, out splitKey, out splitPageNumber);

			if (parentPage == null) //root
			{
				var newRootPage = _tx.AllocatePage(1, PageFlags.Branch | PageFlags.FixedSize);
				newRootPage.FixedSize_ValueSize = _valSize;

				var largePtr = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
				largePtr->RootPageNumber = newRootPage.PageNumber;

				newRootPage.FixedSize_NumberOfEntries = 2;
				var dataStart = (long*)(newRootPage.Base + Constants.PageHeaderSize);
				dataStart[0] = long.MinValue;
				dataStart[1] = page.PageNumber;
				dataStart[2] = splitKey;
				dataStart[3] = splitPageNumber;
				return;
			}

			parentPage = _tx.ModifyPage(parentPage.PageNumber, _parent, parentPage);

			var entriesToMove = parentPage.FixedSize_NumberOfEntries - (parentPage.LastSearchPosition + 1);
			var newEntryPos = parentPage.Base + Constants.PageHeaderSize + ((parentPage.LastSearchPosition + 1) * BranchEntrySize);
			if (entriesToMove > 0)
			{
				StdLib.memmove(newEntryPos + BranchEntrySize,
					newEntryPos,
					entriesToMove * BranchEntrySize);
			}
			parentPage.FixedSize_NumberOfEntries++;
			((long*)newEntryPos)[0] = splitKey;
			((long*)newEntryPos)[1] = splitPageNumber;
		}

		private void ActuallySplitPage(Page page, long key, out long splitKey, out long splitPageNumber)
		{
			var rightPage = _tx.AllocatePage(1, PageFlags.Leaf | PageFlags.FixedSize);
			rightPage.FixedSize_ValueSize = _valSize;
			rightPage.FixedSize_NumberOfEntries = 1;
			splitPageNumber = rightPage.PageNumber;
			// if we are at the end, just create a new one
			if (page.LastSearchPosition == page.FixedSize_NumberOfEntries)
			{
				splitKey = key;
			}
			else // need to actually split it, we'll move 1/4 of the entries to the right hand side
			{
				rightPage.FixedSize_NumberOfEntries = (ushort)(page.FixedSize_NumberOfEntries / 4);
				page.FixedSize_NumberOfEntries -= rightPage.FixedSize_NumberOfEntries;
				MemoryUtils.Copy(rightPage.Base + Constants.PageHeaderSize,
					page.Base + Constants.PageHeaderSize + (page.FixedSize_NumberOfEntries * _entrySize),
					rightPage.FixedSize_NumberOfEntries * _entrySize
					);

				splitKey = ((long*)(rightPage.Base + Constants.PageHeaderSize))[0];
			}
		}

		private void AddEmbeddedEntry(long key, Slice val)
		{
			var ptr = _parent.DirectRead(_treeName);
			var dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
			var header = (FixedSizeTreeHeader.Embedded*)ptr;
			var startingEntryCount = header->NumberOfEntries;
			var pos = BinarySearch(dataStart, startingEntryCount, key, _entrySize);
			var newEntriesCount = startingEntryCount;
			if (_lastMatch != 0)
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
				MemoryUtils.Copy(tmp.TempPagePointer, dataStart, srcCopyStart);
				var newEntryStart = tmp.TempPagePointer + srcCopyStart;
				*((long*)newEntryStart) = key;
				if (val != null)
				{
					val.CopyTo(newEntryStart + sizeof(long));
				}
				MemoryUtils.Copy(newEntryStart + _entrySize, dataStart + srcCopyStart, (startingEntryCount - pos) * _entrySize);

				if (newEntriesCount > _maxEmbeddedEntries)
				{
					// convert to large database
					_flags = FixedSizeTreeHeader.OptionFlags.Large;
					var allocatePage = _tx.AllocatePage(1, PageFlags.Leaf);
					var largeHeader = (FixedSizeTreeHeader.Large*)_parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Large));
					largeHeader->NumberOfEntries = newEntriesCount;
					largeHeader->ValueSize = _valSize;
					largeHeader->Flags = FixedSizeTreeHeader.OptionFlags.Large;
					largeHeader->RootPageNumber = allocatePage.PageNumber;

					allocatePage.Flags = PageFlags.FixedSize | PageFlags.Leaf;
					allocatePage.PageNumber = allocatePage.PageNumber;
					allocatePage.FixedSize_NumberOfEntries = newEntriesCount;
					allocatePage.FixedSize_ValueSize = _valSize;
					MemoryUtils.Copy(allocatePage.Base + Constants.PageHeaderSize, tmp.TempPagePointer,
						newSize);
				}
				else
				{
					byte* newData = _parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Embedded) + newSize);
					header = (FixedSizeTreeHeader.Embedded*)newData;
					header->ValueSize = _valSize;
					header->Flags = FixedSizeTreeHeader.OptionFlags.Embedded;
					header->NumberOfEntries = newEntriesCount;

					MemoryUtils.Copy(newData + sizeof(FixedSizeTreeHeader.Embedded), tmp.TempPagePointer,
						newSize);
				}
			}
		}

		private void AddNewEntry(long key, Slice val)
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
			if (val == null) return;
			val.CopyTo(dataStart + sizeof(long));
		}

		private void BinarySearch(Page page, long val, int size)
		{
			page.LastSearchPosition = BinarySearch(page.Base + Constants.PageHeaderSize, page.FixedSize_NumberOfEntries, val, size);
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
		private long PageValueFor(byte* p, int num, int size)
		{
			var lp = (long*)(p + (num * (size)) + sizeof(long));
			return lp[0];
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
						var childPageNumber = PageValueFor(page.Base + Constants.PageHeaderSize, page.LastSearchPosition, BranchEntrySize);
						page = _tx.GetReadOnlyPage(childPageNumber);
					}
					dataStart = page.Base + Constants.PageHeaderSize;

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

		private void RemoveLargeEntry(long key)
		{
			var page = FindPageFor(key);
			if (page.LastMatch != 0)
				return;
			page = _tx.ModifyPage(page.PageNumber, _parent, page);

			var largeHeader = (FixedSizeTreeHeader.Large*) _parent.DirectAdd(_treeName, sizeof (FixedSizeTreeHeader.Large));
			largeHeader->NumberOfEntries --;
			page.FixedSize_NumberOfEntries--;

			if (_cursor.Count == 0)
			{
				// root page
				if (page.FixedSize_NumberOfEntries <= _maxEmbeddedEntries)
				{
					var ptr = _parent.DirectAdd(_treeName,
						sizeof (FixedSizeTreeHeader.Embedded) + (_entrySize*page.FixedSize_NumberOfEntries));
					var header = (FixedSizeTreeHeader.Embedded*) ptr;
					header->Flags = FixedSizeTreeHeader.OptionFlags.Embedded;
					header->ValueSize = _valSize;
					header->NumberOfEntries = (byte) page.FixedSize_NumberOfEntries;
					_flags = FixedSizeTreeHeader.OptionFlags.Embedded;

					MemoryUtils.Copy(ptr + sizeof (FixedSizeTreeHeader.Embedded),
						page.Base + Constants.PageHeaderSize,
						(_entrySize*page.LastSearchPosition));

					MemoryUtils.Copy(ptr + sizeof (FixedSizeTreeHeader.Embedded) + (_entrySize*page.LastSearchPosition),
						page.Base + Constants.PageHeaderSize + (_entrySize * page.LastSearchPosition) + _entrySize,
						(_entrySize*(page.FixedSize_NumberOfEntries - page.LastSearchPosition)));


					_tx.FreePage(page.PageNumber);
					return;
				}
			}

			if (page.FixedSize_NumberOfEntries > 0)
			{
				StdLib.memmove(page.Base + Constants.PageHeaderSize + (page.LastSearchPosition*_entrySize),
					page.Base + Constants.PageHeaderSize + ((page.LastSearchPosition + 1)*_entrySize),
					_entrySize*(page.FixedSize_NumberOfEntries - page.LastSearchPosition));
				// deleted and we are done
				return;
			}

			// now we need to remove from parent
			_tx.FreePage(page.PageNumber);

			var parentPage = _cursor.Pop();
			var parentPageNumber = parentPage.PageNumber;
			parentPage = _tx.ModifyPage(parentPageNumber, _parent, parentPage);
			parentPage.FixedSize_NumberOfEntries--;
			// branch pages must have at least 2 entries, so this is safe to do
			StdLib.memmove(parentPage.Base + Constants.PageHeaderSize + (parentPage.LastSearchPosition*_entrySize),
				parentPage.Base + Constants.PageHeaderSize + ((parentPage.LastSearchPosition + 1)*_entrySize),
				_entrySize*(parentPage.FixedSize_NumberOfEntries - parentPage.LastSearchPosition));
			
			// it has only one entry, we can delete it and switch with the last child item
			if (parentPage.FixedSize_NumberOfEntries > 1) 
				return;

			var lastChildNumber = *(long*) (parentPage.Base + Constants.PageHeaderSize + sizeof (long));
			var childPage = _tx.GetReadOnlyPage(lastChildNumber);
			MemoryUtils.Copy(parentPage.Base, childPage.Base, parentPage.PageSize);
			parentPage.PageNumber = parentPageNumber;// overwritten in copy
			_tx.FreePage(lastChildNumber);
		}

		private void RemoveEmbeddedEntry(long key)
		{
			byte* ptr = _parent.DirectRead(_treeName);
			var header = (FixedSizeTreeHeader.Embedded*) ptr;
			var startingEntryCount = header->NumberOfEntries;
			var pos = BinarySearch(ptr + sizeof (FixedSizeTreeHeader.Embedded), startingEntryCount, key, _entrySize);
			if (_lastMatch != 0)
			{
				return; // not here, nothing to do
			}
			if (startingEntryCount == 1)
			{
				// only single entry, just remove it
				_flags = null;
				_parent.Delete(_treeName);
				return;
			}

			byte* newData = _parent.DirectAdd(_treeName,
				sizeof (FixedSizeTreeHeader.Embedded) + ((startingEntryCount - 1)*_entrySize));

			int srcCopyStart = pos*_entrySize + sizeof (FixedSizeTreeHeader.Embedded);
			MemoryUtils.Copy(newData, ptr, srcCopyStart);
			header = (FixedSizeTreeHeader.Embedded*) newData;
			header->NumberOfEntries--;
			header->ValueSize = _valSize;
			header->Flags = FixedSizeTreeHeader.OptionFlags.Embedded;
			MemoryUtils.Copy(newData + srcCopyStart, ptr + srcCopyStart + _entrySize, (header->NumberOfEntries - pos)*_entrySize);
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
						var childPageNumber = PageValueFor(page.Base + Constants.PageHeaderSize, page.LastSearchPosition, BranchEntrySize);
						page = _tx.GetReadOnlyPage(childPageNumber);
					}
					dataStart = page.Base + Constants.PageHeaderSize;

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
			}

			return null;
		}
	}
}