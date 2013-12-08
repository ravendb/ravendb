using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Voron.Impl;

namespace Voron.Trees
{
	public unsafe class TreeIterator : IIterator
	{
		private readonly Tree _tree;
		private readonly Transaction _tx;
		private readonly SliceComparer _cmp;
		private Cursor _cursor;
		private Page _currentPage;
		private readonly Slice _currentKey = new Slice(SliceOptions.Key);

		public TreeIterator(Tree tree, Transaction tx, SliceComparer cmp)
		{
			_tree = tree;
			_tx = tx;
			_cmp = cmp;
		}


		public int GetCurrentDataSize()
		{
			return NodeHeader.GetDataSize(_tx, Current);
		}

		public bool Seek(Slice key)
		{
			Lazy<Cursor> lazy;
			_currentPage = _tree.FindPageFor(_tx, key, out lazy);
			_cursor = lazy.Value;
			_cursor.Pop();
			var node = _currentPage.Search(key, _cmp);
			if (node == null)
			{
				return false;
			}
			_currentKey.Set(node);
			return this.ValidateCurrentKey(Current, _cmp);
		}

		public Slice CurrentKey
		{
			get
			{
				if (_currentPage == null || _currentPage.LastSearchPosition >= _currentPage.NumberOfEntries)
					throw new InvalidOperationException("No current page was set");
				return _currentKey;
			}
		}

		public NodeHeader* Current
		{
			get
			{
				if (_currentPage == null || _currentPage.LastSearchPosition >= _currentPage.NumberOfEntries)
					throw new InvalidOperationException("No current page was set");
				return _currentPage.GetNode(_currentPage.LastSearchPosition);
			}
		}

		public bool MovePrev()
		{
			while (true)
			{
				_currentPage.LastSearchPosition--;
				if (_currentPage.LastSearchPosition >= 0)
				{
					// run out of entries, need to select the next page...
					while (_currentPage.IsBranch)
					{
						_cursor.Push(_currentPage);
						var node = _currentPage.GetNode(_currentPage.LastSearchPosition);
						_currentPage = _tx.GetReadOnlyPage(node->PageNumber);
						_currentPage.LastSearchPosition = _currentPage.NumberOfEntries - 1;
					}
					var current = _currentPage.GetNode(_currentPage.LastSearchPosition);
					if (this.ValidateCurrentKey(current, _cmp) == false)
						return false;
					_currentKey.Set(current);
					return true;// there is another entry in this page
				}
				if (_cursor.PageCount == 0)
					break;
				_currentPage = _cursor.Pop();
			}
			_currentPage = null;
			return false;
		}

		public bool MoveNext()
		{
			while (true)
			{
				_currentPage.LastSearchPosition++;
				if (_currentPage.LastSearchPosition < _currentPage.NumberOfEntries)
				{
					// run out of entries, need to select the next page...
					while (_currentPage.IsBranch)
					{
						_cursor.Push(_currentPage);
						var node = _currentPage.GetNode(_currentPage.LastSearchPosition);
						_currentPage = _tx.GetReadOnlyPage(node->PageNumber);

						_currentPage.LastSearchPosition = 0;
					}
					var current = _currentPage.GetNode(_currentPage.LastSearchPosition);
					if (this.ValidateCurrentKey(current, _cmp) == false)
						return false;
					_currentKey.Set(current);
					return true;// there is another entry in this page
				}
				if (_cursor.PageCount == 0)
					break;
				_currentPage = _cursor.Pop();
			}
			_currentPage = null;
			return false;
		}

		public bool Skip(int count)
		{
			if (count != 0)
			{
				var moveMethod = (count > 0) ? (Func<bool>)MoveNext : MovePrev;

				for (int i = 0; i < Math.Abs(count); i++)
				{
					if (!moveMethod()) break;
				}
			}

			return _currentPage != null && this.ValidateCurrentKey(Current, _cmp);
		}

		public Stream CreateStreamForCurrent()
		{
			return NodeHeader.Stream(_tx, Current);
		}

		public void Dispose()
		{
		}

		public Slice RequiredPrefix { get; set; }

		public Slice MaxKey { get; set; }

	    public long TreeRootPage
	    {
            get { return  this._tree.State.RootPageNumber; }
	    }
	}

	public static class IteratorExtensions
	{
		public unsafe static bool ValidateCurrentKey(this IIterator self, NodeHeader* node, SliceComparer cmp)
		{
			if (self.RequiredPrefix != null)
			{
				var currentKey = new Slice(node);
				if (currentKey.StartsWith(self.RequiredPrefix, cmp) == false)
					return false;
			}
			if (self.MaxKey != null)
			{
				var currentKey = new Slice(node);
				if (currentKey.Compare(self.MaxKey, cmp) >= 0)
					return false;
			}
			return true;
		}
	}
}