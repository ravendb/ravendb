using System;
using Nevar.Impl;

namespace Nevar.Trees
{
	public unsafe class Iterator : IDisposable
	{
		private readonly Tree _tree;
		private readonly Transaction _tx;
		private readonly SliceComparer _cmp;
		private readonly Cursor _cursor;
		private Page _currentPage;

		public Iterator(Tree tree, Transaction tx, SliceComparer cmp)
		{
			_tree = tree;
			_tx = tx;
			_cmp = cmp;
			_cursor = tx.NewCursor(_tree);
		}

		public bool Seek(Slice key)
		{
			_currentPage = _tree.FindPageFor(_tx, key, _cursor);
			_cursor.Pop();
			var node = _currentPage.Search(key, _cmp);
			if (node == null)
			{
				return false;
			}

			return ValidateCurrentKey();
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

		public bool MoveNext()
		{
			while (true)
			{
				_currentPage.LastSearchPosition++;
				if (_currentPage.LastSearchPosition < _currentPage.NumberOfEntries)
				{
					// run out of entries, need to select the next page...
					if (_currentPage.IsBranch)
					{
						_cursor.Push(_currentPage);
						var node = _currentPage.GetNode(_currentPage.LastSearchPosition);
                        _currentPage = _tx.GetReadOnlyPage(node->PageNumber);
						_currentPage.LastSearchPosition = 0;
					}
					if (ValidateCurrentKey() == false)
						return false;
					return true;// there is another entry in this page
				}
				if (_cursor.Pages.Count == 0)
					break;
				_currentPage = _cursor.Pop();
			}
			_currentPage = null;
			return false;
		}

		private bool ValidateCurrentKey()
		{
			if (RequiredPrefix != null)
			{
				var currentKey = new Slice(Current);
				if (currentKey.StartsWith(RequiredPrefix, _cmp) == false)
					return false;
			}
			if (MaxKey != null)
			{
				var currentKey = new Slice(Current);
				if (currentKey.Compare(MaxKey, _cmp) >= 0)
					return false;
			}
			return true;
		}

		public void Dispose()
		{
		    _cursor.Dispose();
		}

		public Slice RequiredPrefix { get; set; }

		public Slice MaxKey { get; set; }
	}
}