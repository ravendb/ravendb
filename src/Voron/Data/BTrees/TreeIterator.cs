using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Platform;
using Voron.Data.Compression;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    public unsafe class TreeIterator : IIterator
    {
        private readonly Tree _tree;
        private readonly LowLevelTransaction _tx;
        private readonly bool _prefetch;

        private TreeCursor _cursor;
        private TreePage _currentPage;
        private bool _disposed;

        private DecompressedLeafPage _decompressedPage;

        public event Action<IIterator> OnDisposal;

        private Slice _currentKey = default(Slice);
        private Slice _currentInternalKey = default(Slice);

        private ByteStringContext.ExternalScope _prevKeyScope = default(ByteStringContext.ExternalScope);

        public TreeIterator(Tree tree, LowLevelTransaction tx, bool prefetch)
        {
            _tree = tree;
            _tx = tx;
            _prefetch = prefetch;
        }

        public int GetCurrentDataSize()
        {
            if(_disposed)
                throw new ObjectDisposedException("TreeIterator " + _tree.Name);
            return _tree.GetDataSize(Current);
        }

        public bool Seek(Slice key)
        {
            if (_disposed)
                throw new ObjectDisposedException("TreeIterator " + _tree.Name);

            TreeNodeHeader* node;
            TreeCursorConstructor constructor;
            _currentPage = _tree.FindPageFor(key, node: out node, cursor: out constructor, allowCompressed: _tree.IsLeafCompressionSupported);
            if (_currentPage.IsCompressed)
            {
                DecompressedCurrentPage();
                node = _currentPage.Search(_tx, key);
            }
            
            _cursor = constructor.Build(key);
            _cursor.Pop();

            if (node != null)
            {
                _prevKeyScope.Dispose();
                _prevKeyScope = TreeNodeHeader.ToSlicePtr(_tx.Allocator, node, out _currentInternalKey);
                _currentKey = _currentInternalKey; 

                if (DoRequireValidation)
                    return this.ValidateCurrentKey(_tx, Current);
                return true;
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

        public TreeNodeHeader* Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
                        // In here we will also have the 'current' page (even if we are traversing a compressed node).
                        if (_prefetch)
                            MaybePrefetchPagesReferencedBy(_currentPage);

                        _cursor.Push(_currentPage);
                        var node = _currentPage.GetNode(_currentPage.LastSearchPosition);
                        _currentPage = _tree.GetReadOnlyTreePage(node->PageNumber);

                        if (_currentPage.IsCompressed)
                            DecompressedCurrentPage();

                        _currentPage.LastSearchPosition = _currentPage.NumberOfEntries - 1;
                    }

                    // We should be prefetching data pages down here.
                    if (_prefetch)
                        MaybePrefetchPagesReferencedBy(_currentPage);

                    var current = _currentPage.GetNode(_currentPage.LastSearchPosition);

                    if (DoRequireValidation && this.ValidateCurrentKey(_tx, current) == false)
                        return false;

                    _prevKeyScope.Dispose();
                    _prevKeyScope = TreeNodeHeader.ToSlicePtr(_tx.Allocator, current, out _currentInternalKey);
                    _currentKey = _currentInternalKey;

                    return true;// there is another entry in this page
                }
                if (_cursor.PageCount == 0)
                    break;
                _currentPage = _cursor.Pop();
            }
            _currentPage = null;
            return false;
        }

        private void MaybePrefetchPagesReferencedBy(TreePage page)
        {
            if (PlatformDetails.CanPrefetch)
            {
                _tx.DataPager.MaybePrefetchMemory(page.GetPagesReferencedBy());
            }
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
                        // In here we will also have the 'current' page (even if we are traversing a compressed node).
                        if (_prefetch)
                            MaybePrefetchPagesReferencedBy(_currentPage);

                        _cursor.Push(_currentPage);
                        var node = _currentPage.GetNode(_currentPage.LastSearchPosition);
                        _currentPage = _tree.GetReadOnlyTreePage(node->PageNumber);

                        if (_currentPage.IsCompressed)
                            DecompressedCurrentPage();

                        _currentPage.LastSearchPosition = 0;
                    }

                    // We should be prefetching data pages down here.
                    if (_prefetch)
                        MaybePrefetchPagesReferencedBy(_currentPage);

                    var current = _currentPage.GetNode(_currentPage.LastSearchPosition);

                    if (DoRequireValidation && this.ValidateCurrentKey(_tx, current) == false)
                        return false;

                    _prevKeyScope.Dispose();
                    _prevKeyScope = TreeNodeHeader.ToSlicePtr(_tx.Allocator, current, out _currentInternalKey);
                    _currentKey = _currentInternalKey;
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

            if ( DoRequireValidation )
                return _currentPage != null && this.ValidateCurrentKey(_tx, Current);
            return _currentPage != null;
        }

        public ValueReader CreateReaderForCurrent()
        {
            return _tree.GetValueReaderFromHeader(Current);
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            if(RequiredPrefix.HasValue)
                RequiredPrefix.Release(_tx.Allocator);
            if (MaxKey.HasValue)
                MaxKey.Release(_tx.Allocator);
            _prevKeyScope.Dispose();
            _cursor?.Dispose();
            _decompressedPage?.Dispose();
            OnDisposal?.Invoke(this);
        }


        private bool _requireValidation;
        public bool DoRequireValidation
        {
            get { return _requireValidation; }
        }

        private Slice _requiredPrefix;

        public Slice RequiredPrefix
        {
            get { return _requiredPrefix; }
        }

        public void SetRequiredPrefix(Slice prefix)
        {
            _requiredPrefix = prefix.Clone(_tx.Allocator); // make sure the prefix slice won't become invalid during iterator usage
            _requireValidation = _maxKey.HasValue || _requiredPrefix.HasValue;
        }

        private Slice _maxKey;
        public Slice MaxKey
        {
            get { return _maxKey; }
            set
            {
                _maxKey = value;
                _requireValidation = _maxKey.HasValue || _requiredPrefix.HasValue;
            }
        }

        public long TreeRootPage
        {
            get { return  this._tree.State.RootPageNumber; }
        }

        private void DecompressedCurrentPage()
        {
            Debug.Assert(_tree.IsLeafCompressionSupported);

            _decompressedPage?.Dispose();

            _currentPage = _decompressedPage = _tree.DecompressPage(_currentPage);
        }
    }

    public static class IteratorExtensions
    {
        public static IEnumerable<string> DumpValues(this IIterator self)
        {                      
            if (self.Seek(Slices.BeforeAllKeys) == false)
                yield break;

            do
            {
                yield return self.CurrentKey.ToString();
            }
            while (self.MoveNext());
        }
        
        public static unsafe bool ValidateCurrentKey<T>(this T self, LowLevelTransaction tx, TreeNodeHeader* node) where T : IIterator
        {
            Slice currentKey;
            using (TreeNodeHeader.ToSlicePtr(tx.Allocator, node, out currentKey))
            {
                if (self.RequiredPrefix.HasValue)
                {
                    if (SliceComparer.StartWith(currentKey, self.RequiredPrefix) == false)
                        return false;
                }
                if (self.MaxKey.HasValue)
                {
                    if (SliceComparer.CompareInline(currentKey, self.MaxKey) >= 0)
                        return false;
                }
            }
            return true;
        }
    }
}
