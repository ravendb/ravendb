using System;
using Voron.Impl;

namespace Voron.Trees
{
	public unsafe class PageIterator : IIterator
	{
		private readonly Page _page;
		private Slice _currentKey = new Slice(SliceOptions.Key);
		private MemorySlice _currentInternalKey;

		public PageIterator(Page page)
		{
			_page = page;
			_currentInternalKey = page.CreateNewEmptyKey();
		}

		public void Dispose()
		{
			
		}

		public bool Seek(Slice key)
		{
			var current = _page.Search(key);
			if (current == null)
				return false;

			_page.SetNodeKey(current, ref _currentInternalKey);
			_currentKey = _currentInternalKey.ToSlice();
			return this.ValidateCurrentKey(current, _page);
		}

		public NodeHeader* Current
		{
			get
			{
				if (_page.LastSearchPosition< 0  || _page.LastSearchPosition >= _page.NumberOfEntries)
					throw new InvalidOperationException("No current page was set");
				return _page.GetNode(_page.LastSearchPosition);
			}
		}


		public Slice CurrentKey
		{
			get
			{
				if (_page.LastSearchPosition < 0 || _page.LastSearchPosition >= _page.NumberOfEntries)
					throw new InvalidOperationException("No current page was set");
				return _currentKey;
			}
		}
		public int GetCurrentDataSize()
		{
			return Current->DataSize;
		}


		public Slice RequiredPrefix { get; set; }
		public Slice MaxKey { get; set; }

		public bool MoveNext()
		{
			_page.LastSearchPosition++;
			return TrySetPosition();
		}

		public bool MovePrev()
		{
			_page.LastSearchPosition--;

			return TrySetPosition();

		}

		public bool Skip(int count)
		{
			_page.LastSearchPosition += count;
			
			return TrySetPosition();
		}

		private bool TrySetPosition()
		{
			if (_page.LastSearchPosition < 0 || _page.LastSearchPosition >= _page.NumberOfEntries)
				return false;

			var current = _page.GetNode(_page.LastSearchPosition);
			if (this.ValidateCurrentKey(current, _page) == false)
			{
				return false;
			}
			_page.SetNodeKey(current, ref _currentInternalKey);
			_currentKey = _currentInternalKey.ToSlice();
			return true;
		}

		public ValueReader CreateReaderForCurrent()
		{
			var node = Current;
			return new ValueReader((byte*)node + node->KeySize + Constants.NodeHeaderSize, node->DataSize);
		}

		public TStruct ReadStructForCurrent<TStruct>() where TStruct : struct
		{
			throw new InvalidOperationException("Multi trees do not support reading/writing structs");
		}
	}
}