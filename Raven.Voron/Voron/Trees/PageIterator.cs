using System;
using System.Collections.Generic;
using Voron.Impl;

namespace Voron.Trees
{
	public unsafe class PageIterator : IIterator
	{
		private readonly SliceComparer _cmp;
		private readonly Page _page;
		private Slice _currentKey = new Slice(SliceOptions.Key);

		public PageIterator(SliceComparer cmp, Page page)
		{
			this._cmp = cmp;
			this._page = page;
		}

		public void Dispose()
		{
			
		}

		public bool Seek(Slice key)
		{
			var current = _page.Search(key, _cmp);
			if (current == null)
				return false;
			_currentKey.Set(current);
			return this.ValidateCurrentKey(current, _cmp);
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
			if (this.ValidateCurrentKey(current, _cmp) == false)
			{
				return false;
			}
			_currentKey.Set(current);
			return true;
		}

		public ValueReader CreateReaderForCurrent()
		{
			var node = Current;
			return new ValueReader((byte*)node + node->KeySize + Constants.NodeHeaderSize, node->DataSize);
		}
	}
}