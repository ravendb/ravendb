using System;
using System.Collections.Generic;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    public unsafe class TreeIterator : IIterator
    {
        private readonly Tree _tree;
        private readonly LowLevelTransaction _tx;
        private TreeCursor _cursor;
        private TreePage _currentPage;
        private Slice _currentKey = new Slice(SliceOptions.Key);
        private Slice _currentInternalKey;
        private bool _disposed;

        public event Action<IIterator> OnDisposal;

        public TreeIterator(Tree tree, LowLevelTransaction tx)
        {
            _tree = tree;
            _tx = tx;

            _currentInternalKey = new Slice(SliceOptions.Key); 				
        }

        public int GetCurrentDataSize()
        {
            if(_disposed)
                throw new ObjectDisposedException("TreeIterator " + _tree.Name);
            return TreeNodeHeader.GetDataSize(_tx, Current);
        }

        public bool Seek(Slice key)
        {
            if (_disposed)
                throw new ObjectDisposedException("TreeIterator " + _tree.Name);

            TreeNodeHeader* node;
            Func<TreeCursor> cursorConstructor;
            _currentPage = _tree.FindPageFor(key, out node, out cursorConstructor);
            _cursor = cursorConstructor();
            _cursor.Pop();

            if (node != null)
            {
                _currentPage.SetNodeKey(node, ref _currentInternalKey);
                _currentKey = _currentInternalKey.ToSlice();
                return this.ValidateCurrentKey(Current, _currentPage);
            }
            
            // The key is not found in the db, but we are Seek()ing for equals or starts with.
            // We know that the exact value isn't there, but it is possible that the next page has values 
            // that is actually greater than the key, so we need to check it as well.

            _currentPage.LastSearchPosition = _currentPage.NumberOfEntries; // force next MoveNext to move to the next _page_.
            return MoveNext();
        }

        public Slice CurrentKey
        {
            get
            {
                if (_disposed)
                    throw new ObjectDisposedException("TreeIterator " + _tree.Name);

                if (_currentPage == null)
                    throw new InvalidOperationException("No current page was set");

                if (_currentPage.LastSearchPosition >= _currentPage.NumberOfEntries)
                    throw new InvalidOperationException(string.Format("Current page is invalid. Search position ({0}) exceeds number of entries ({1}). Page: {2}.", _currentPage.LastSearchPosition, _currentPage.NumberOfEntries, _currentPage));
                    
                return _currentKey;
            }
        }

        /// <summary>
        /// Deletes the current key/value pair and returns true if there is 
        /// another key after it
        /// </summary>
        public bool DeleteCurrentAndMoveNext()
        {
            var currentKey = CurrentKey;
            _tree.Delete(currentKey);
            return Seek(currentKey);
        }

        public TreeNodeHeader* Current
        {
            get
            {
                if (_disposed)
                    throw new ObjectDisposedException("TreeIterator " + _tree.Name);

                if (_currentPage == null)
                    throw new InvalidOperationException("No current page was set");

                if (_currentPage.LastSearchPosition >= _currentPage.NumberOfEntries)
                    throw new InvalidOperationException(string.Format("Current page is invalid. Search position ({0}) exceeds number of entries ({1}). Page: {2}.", _currentPage.LastSearchPosition, _currentPage.NumberOfEntries, _currentPage));
                    
                return _currentPage.GetNode(_currentPage.LastSearchPosition);
            }
        }

        public bool MovePrev()
        {
            if (_disposed)
                throw new ObjectDisposedException("TreeIterator " + _tree.Name);
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
                        _currentPage = _tx.GetReadOnlyTreePage(node->PageNumber);
                        _currentPage.LastSearchPosition = _currentPage.NumberOfEntries - 1;
                    }
                    var current = _currentPage.GetNode(_currentPage.LastSearchPosition);
                    if (this.ValidateCurrentKey(current, _currentPage) == false)
                        return false;

                    _currentPage.SetNodeKey(current, ref _currentInternalKey);
                    _currentKey = _currentInternalKey.ToSlice();
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
            if (_disposed)
                throw new ObjectDisposedException("TreeIterator " + _tree.Name);

            while (_currentPage != null)
            {
                _currentPage.LastSearchPosition++;
                if (_currentPage.LastSearchPosition < _currentPage.NumberOfEntries)
                {
                    // run out of entries, need to select the next page...
                    while (_currentPage.IsBranch)
                    {
                        _cursor.Push(_currentPage);
                        var node = _currentPage.GetNode(_currentPage.LastSearchPosition);
                        _currentPage = _tx.GetReadOnlyTreePage(node->PageNumber);

                        _currentPage.LastSearchPosition = 0;
                    }
                    var current = _currentPage.GetNode(_currentPage.LastSearchPosition);
                    if (this.ValidateCurrentKey(current, _currentPage) == false)
                        return false;

                    _currentPage.SetNodeKey(current, ref _currentInternalKey);
                    _currentKey = _currentInternalKey.ToSlice();
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

            return _currentPage != null && this.ValidateCurrentKey(Current, _currentPage);
        }

        public ValueReader CreateReaderForCurrent()
        {
            return TreeNodeHeader.Reader(_tx, Current);
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;

            _cursor.Dispose();
            OnDisposal?.Invoke(this);
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
        public static IEnumerable<string> DumpValues(this IIterator self)
        {
            if (self.Seek(Slice.BeforeAllKeys) == false)
                yield break;

            do
            {
                yield return self.CurrentKey.ToString();
            } while (self.MoveNext());
        }

        public unsafe static bool ValidateCurrentKey(this IIterator self, TreeNodeHeader* node, TreePage page)
        {
            if (self.RequiredPrefix != null)
            {
                var currentKey = page.GetNodeKey(node);
                if (currentKey.StartsWith(self.RequiredPrefix) == false)
                    return false;
            }
            if (self.MaxKey != null)
            {
                var currentKey = page.GetNodeKey(node);
                if (currentKey.Compare(self.MaxKey) >= 0)
                    return false;
            }
            return true;
        }
    }
}