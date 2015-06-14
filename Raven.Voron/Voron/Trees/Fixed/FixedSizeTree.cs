// -----------------------------------------------------------------------
//  <copyright file="FixedSizeTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.CompilerServices;
using Voron.Impl;
using Voron.Impl.FileHeaders;
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

		public void Add(long key, Slice val = null)
		{
			byte* ptr;
			FixedSizeTreeHeader.Embedded* header;
			byte* dataStart;
			switch (_flags)
			{
				case null:
					// new, just create it & go
					ptr = _parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Embedded) + _entrySize);
					header = (FixedSizeTreeHeader.Embedded*)ptr;
					header->Flags = FixedSizeTreeHeader.OptionFlags.Embedded;
					header->ValueSize = _valSize;
					header->NumberOfEntries = 1;
					_flags = FixedSizeTreeHeader.OptionFlags.Embedded;

					dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
					*(long*)(dataStart) = key;
					if (val != null)
					{
						if(val.Size != _valSize)
							throw new InvalidOperationException("The value size must be " + _valSize + " but was " + val.Size);
						val.CopyTo(dataStart + sizeof(long));
					}
					break;
				case FixedSizeTreeHeader.OptionFlags.Embedded:
					ptr = _parent.DirectRead(_treeName);
					dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
					header = (FixedSizeTreeHeader.Embedded*)ptr;
					var startingEntryCount = header->NumberOfEntries;
					var pos = BinarySearch(ptr + sizeof(FixedSizeTreeHeader.Embedded), startingEntryCount, key);
					var newEntriesCount = startingEntryCount;
					if (startingEntryCount == pos || KeyFor(dataStart, pos) != key)
					{
						newEntriesCount++; // new entry, need more space
						if (newEntriesCount > _maxEmbeddedEntries)
						{
							// convert to large mode
						}
					}

					byte* newData = _parent.DirectAdd(_treeName, sizeof(FixedSizeTreeHeader.Embedded) + (newEntriesCount * _entrySize));
					int srcCopyStart = pos * _entrySize + sizeof(FixedSizeTreeHeader.Embedded);
					MemoryUtils.CopyInline(newData, ptr, srcCopyStart);
					header = (FixedSizeTreeHeader.Embedded*)newData;
					header->NumberOfEntries = newEntriesCount;
					var newEntryStart = newData + srcCopyStart;
					*((long*)newEntryStart) = key;
					if (val != null)
					{
						if(val.Size != _valSize)
							throw new InvalidOperationException("The value size must be " + _valSize + " but was " + val.Size);
						val.CopyTo(newEntryStart + sizeof(long));
					}
					MemoryUtils.CopyInline(newEntryStart + _entrySize, ptr + srcCopyStart, (startingEntryCount - pos) * _entrySize);
					break;
			}
		}

		private int BinarySearch(byte* p, int len, long val)
		{
			int low = 0;
			int high = len - 1;
			int position = 0;

			long res = 0;
			while (low <= high)
			{
				position = (low + high) >> 1;

				res = val - KeyFor(p, position);
				if (res == 0)
					break;

				if (res < 0)
					low = position + 1;
				else
					high = position - 1;
			}
			if (res > 0)
				return position + 1;
			return position;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private long KeyFor(byte* p, int num)
		{
			var lp = (long*)(p + (num * _entrySize));
			return lp[0];
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
					var pos = BinarySearch(dataStart, header->NumberOfEntries, key);
					return KeyFor(dataStart, pos) == key;
			}

			return false;
		}

		public void Remove(long key)
		{
			switch (_flags)
			{
				case null:
					// nothing to do
					break;
				case FixedSizeTreeHeader.OptionFlags.Embedded:
					byte* ptr = _parent.DirectRead(_treeName);
					byte* dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
					FixedSizeTreeHeader.Embedded* header = (FixedSizeTreeHeader.Embedded*)ptr;
					var startingEntryCount = header->NumberOfEntries;
					var pos = BinarySearch(ptr + sizeof(FixedSizeTreeHeader.Embedded), startingEntryCount, key);
					if (startingEntryCount == pos || KeyFor(dataStart, pos) != key)
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
						sizeof(FixedSizeTreeHeader.Embedded) + ((startingEntryCount-1) * _entrySize));

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
					if (pos == header->NumberOfEntries || KeyFor(dataStart, pos) != key)
						return null;
					return new Slice(dataStart + (pos*_entrySize) + sizeof (long), _valSize);
			}

			return null;
		}
	}
}