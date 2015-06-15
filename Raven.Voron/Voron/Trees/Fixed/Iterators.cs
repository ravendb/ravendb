// -----------------------------------------------------------------------
//  <copyright file="Iterators.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Voron.Impl.FileHeaders;

namespace Voron.Trees.Fixed
{
	public unsafe partial class FixedSizeTree
	{
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
						_pos = _fst.BinarySearch(_dataStart, _header->NumberOfEntries, key, _fst._entrySize);
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
					return _fst.KeyFor(_dataStart, _pos, _fst._entrySize);
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

	}
}