// -----------------------------------------------------------------------
//  <copyright file="FixedSizeTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics.Eventing.Reader;
using System.Runtime.CompilerServices;
using System.Runtime.Remoting.Messaging;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Trees.Fixed
{
	public unsafe class FixedSizeTree
	{
		private readonly Transaction _tx;
		private readonly Tree _parent;
		private readonly Slice _treeName;
		private readonly byte _valSize;
		private readonly int _entrySize;
		private readonly int _maxEmbeddedEntries;
		private FixedSizeTreeHeader.OptionFlags? _flags;

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

		public int EntrySize { get { return _entrySize; } }

		public void Add(long key, Slice val = null)
		{
			if (_valSize == 0 && val != null)
				throw new InvalidOperationException("When the value size is zero, no value can be specified");
			if (_valSize != 0 && val == null)
				throw new InvalidOperationException("When the value size is not zero, the value must be specified");
			if(val != null && val.Size != _valSize)
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
					var ptr = (FixedSizeTreeHeader.Large*) _parent.DirectRead(_treeName);
					var page = _tx.GetReadOnlyPage(ptr->RootPageNumber);
					

					break;
			}
		}

		private void AddEmbeddedEntry(long key, Slice val)
		{
			var ptr = _parent.DirectRead(_treeName);
			var dataStart = ptr + sizeof (FixedSizeTreeHeader.Embedded);
			var header = (FixedSizeTreeHeader.Embedded*) ptr;
			var startingEntryCount = header->NumberOfEntries;
			var pos = BinarySearch(dataStart, startingEntryCount, key);
			var newEntriesCount = startingEntryCount;
			if (lastMatch != 0)
			{
				newEntriesCount++; // new entry, need more space
			}
			if (lastMatch > 0)
				pos++; // we need to put this _after_ the previous one
			var newSize = (newEntriesCount*_entrySize);
			TemporaryPage tmp;
			using (_tx.Environment.GetTemporaryPage(_tx, out tmp))
			{
				int srcCopyStart = pos*_entrySize;
				MemoryUtils.CopyInline(tmp.TempPagePointer, dataStart, srcCopyStart);
				var newEntryStart = tmp.TempPagePointer + srcCopyStart;
				*((long*) newEntryStart) = key;
				if (val != null)
				{
					val.CopyTo(newEntryStart + sizeof (long));
				}
				MemoryUtils.CopyInline(newEntryStart + _entrySize, dataStart + srcCopyStart, (startingEntryCount - pos)*_entrySize);

				if (newEntriesCount > _maxEmbeddedEntries)
				{
					// convert to large database
					_flags = FixedSizeTreeHeader.OptionFlags.Large;
					var allocatePage = _tx.AllocatePage(1, PageFlags.Leaf);
					var largeHeader = (FixedSizeTreeHeader.Large*) _parent.DirectAdd(_treeName, sizeof (FixedSizeTreeHeader.Large));
					largeHeader->NumberOfEntries = newEntriesCount;
					largeHeader->ValueSize = _valSize;
					largeHeader->Flags = FixedSizeTreeHeader.OptionFlags.Large;
					largeHeader->RootPageNumber = allocatePage.PageNumber;

					var fixedSizePage = new FixedSizePage(this, allocatePage.Base);
					fixedSizePage.Header->Flags = PageFlags.FixedSize | PageFlags.Leaf;
					fixedSizePage.Header->PageNumber = allocatePage.PageNumber;
					fixedSizePage.Header->NumberOfEntries = newEntriesCount;
					fixedSizePage.Header->ValueSize = _valSize;
					MemoryUtils.CopyInline(fixedSizePage.Data, tmp.TempPagePointer,
						newSize);
				}
				else
				{
					byte* newData = _parent.DirectAdd(_treeName, sizeof (FixedSizeTreeHeader.Embedded) + newSize);
					header = (FixedSizeTreeHeader.Embedded*) newData;
					header->ValueSize = _valSize;
					header->Flags = FixedSizeTreeHeader.OptionFlags.Embedded;
					header->NumberOfEntries = newEntriesCount;

					MemoryUtils.CopyInline(newData + sizeof (FixedSizeTreeHeader.Embedded), tmp.TempPagePointer,
						newSize);
				}
			}
		}

		private void AddNewEntry(long key, Slice val)
		{
			// new, just create it & go
			var ptr = _parent.DirectAdd(_treeName, sizeof (FixedSizeTreeHeader.Embedded) + _entrySize);
			var header = (FixedSizeTreeHeader.Embedded*) ptr;
			header->Flags = FixedSizeTreeHeader.OptionFlags.Embedded;
			header->ValueSize = _valSize;
			header->NumberOfEntries = 1;
			_flags = FixedSizeTreeHeader.OptionFlags.Embedded;

			byte* dataStart = ptr + sizeof (FixedSizeTreeHeader.Embedded);
			*(long*) (dataStart) = key;
			if (val == null) return;
			val.CopyTo(dataStart + sizeof (long));
		}

		private long lastMatch;
		private int BinarySearch(byte* p, int len, long val)
		{
			int low = 0;
			int high = len - 1;

			switch (val)
			{
				case Int64.MinValue:
					lastMatch = -1;
					return low;
				case Int64.MaxValue:
					lastMatch = 1;
					return high;
			}

			int position = 0;
			while (low <= high)
			{
				position = (low + high) >> 1;

				lastMatch = val - KeyFor(p, position);
				if (lastMatch == 0)
					break;

				if (lastMatch > 0)
					low = position + 1;
				else
					high = position - 1;
			}
			return position;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private long KeyFor(byte* p, int num)
		{
			var lp = (long*)(p + (num * _entrySize));
			return lp[0];
		}

		public interface IFixedSizeIterator
		{
			bool Seek(long key);
			long Key { get; }
			Slice Value { get; }
			bool MoveNext();
		}

		public class NullIterator : IFixedSizeIterator
		{
			public bool Seek(long key)
			{
				return false;
			}

			public long Key { get { throw new InvalidOperationException("Invalid position, cannot read past end of tree"); } }
			public Slice Value { get { throw new InvalidOperationException("Invalid position, cannot read past end of tree"); } }
			public bool MoveNext()
			{
				return false;
			}
		}

		public class EmbeddedIterator : IFixedSizeIterator
		{
			private readonly FixedSizeTree _fst;
			private int _pos;
			private readonly FixedSizeTreeHeader.Embedded* _header;
			private readonly byte* _dataStart;

			public EmbeddedIterator(FixedSizeTree fst)
			{
				_fst = fst;
				var ptr = _fst._parent.DirectRead(_fst._treeName);
				_header = (FixedSizeTreeHeader.Embedded*)ptr;
				_dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
			}

			public bool Seek(long key)
			{
				switch (_fst._flags)
				{
					case FixedSizeTreeHeader.OptionFlags.Embedded:
						_pos = _fst.BinarySearch(_dataStart, _header->NumberOfEntries, key);
						return _pos != _header->NumberOfEntries;
					case null:
						return false;
				}
				return false;
			}

			public long Key
			{
				get
				{
					if (_pos == _header->NumberOfEntries)
						throw new InvalidOperationException("Invalid position, cannot read past end of tree");
					return _fst.KeyFor(_dataStart, _pos);
				}
			}

			public Slice Value
			{
				get
				{
					if (_pos == _header->NumberOfEntries)
						throw new InvalidOperationException("Invalid position, cannot read past end of tree");

					return new Slice(_dataStart + (_pos * _fst._entrySize) + sizeof(long), _fst._valSize);
				}
			}

			public bool MoveNext()
			{
				return ++_pos < _header->NumberOfEntries;
			}
		}

		public bool Contains(long key)
		{
			switch (_flags)
			{
				case null:
					return false;
				case FixedSizeTreeHeader.OptionFlags.Embedded:
					var ptr = _parent.DirectRead(_treeName);
					var header = (FixedSizeTreeHeader.Embedded*)ptr;
					var dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
					BinarySearch(dataStart, header->NumberOfEntries, key);
					return lastMatch == 0;
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
					byte* ptr = _parent.DirectRead(_treeName);
					var header = (FixedSizeTreeHeader.Embedded*)ptr;
					var startingEntryCount = header->NumberOfEntries;
					var pos = BinarySearch(ptr + sizeof(FixedSizeTreeHeader.Embedded), startingEntryCount, key);
					if (lastMatch != 0)
					{
						return;  // not here, nothing to do
					}
					if (startingEntryCount == 1)
					{
						// only single entry, just remove it
						_flags = null;
						_parent.Delete(_treeName);
						return;
					}

					byte* newData = _parent.DirectAdd(_treeName,
						sizeof(FixedSizeTreeHeader.Embedded) + ((startingEntryCount - 1) * _entrySize));

					int srcCopyStart = pos * _entrySize + sizeof(FixedSizeTreeHeader.Embedded);
					MemoryUtils.CopyInline(newData, ptr, srcCopyStart);
					header = (FixedSizeTreeHeader.Embedded*)newData;
					header->NumberOfEntries--;
					MemoryUtils.CopyInline(newData + srcCopyStart, ptr + srcCopyStart + _entrySize, (header->NumberOfEntries - pos) * _entrySize);
					break;
			}
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
					var pos = BinarySearch(dataStart, header->NumberOfEntries, key);
					if (lastMatch != 0)
						return null;
					return new Slice(dataStart + (pos * _entrySize) + sizeof(long), _valSize);
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