using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
using Voron.Data.Compression;
using Voron.Impl;

using static Sparrow.DisposableExceptions;
using static Sparrow.PortableExceptions;

namespace Voron.Data.BTrees
{
    public sealed unsafe class TreeIterator : IIterator, IDisposableQueryable
    {
        private readonly Tree _tree;
        private readonly LowLevelTransaction _tx;
        private readonly bool _prefetch;

        private TreeCursor _cursor;
        private TreePage _currentPage;
        
        private bool _isDisposed;
        bool IDisposableQueryable.IsDisposed => _isDisposed;

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
            ThrowIfDisposedOnDebug(this, "TreeIterator " + _tree.Name);
            return _tree.GetDataSize(Current);
        }

        public bool Seek(Slice key)
        {
            ThrowIfDisposedOnDebug(this, "TreeIterator " + _tree.Name);

            _currentPage = _tree.FindPageFor(key, node: out TreeNodeHeader* node, 
                cursor: out TreeCursorConstructor constructor,
                allowCompressed: _tree.IsLeafCompressionSupported);
            
            if (_currentPage.IsCompressed)
            {
                DecompressedCurrentPage();
                node = _currentPage.Search(_tx, key);
            }

            constructor.Build(key, out _cursor);
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
                ThrowIfDisposedOnDebug(this, "TreeIterator " + _tree.Name);

                ThrowIfNull<InvalidOperationException>(_currentPage, "No current page was set");
                ThrowIf<InvalidOperationException>(_currentPage.LastSearchPosition >= _currentPage.NumberOfEntries,
                    $"Current page is invalid. Search position ({_currentPage.LastSearchPosition}) exceeds number of entries ({_currentPage.NumberOfEntries}). " +
                    $"Page: {_currentPage}.");

                return _currentKey;
            }
        }

        public TreeNodeHeader* Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                ThrowIfDisposedOnDebug(this, "TreeIterator " + _tree.Name);

                ThrowIfNull<InvalidOperationException>(_currentPage, "No current page was set");
                ThrowIf<InvalidOperationException>(_currentPage.LastSearchPosition >= _currentPage.NumberOfEntries,
                    $"Current page is invalid. Search position ({_currentPage.LastSearchPosition}) exceeds number of entries ({_currentPage.NumberOfEntries}). " +
                    $"Page: {_currentPage}.");
                
                return _currentPage.GetNode(_currentPage.LastSearchPosition);
            }
        }

        public bool MovePrev()
        {
            ThrowIfDisposedOnDebug(this, "TreeIterator " + _tree.Name);
            
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
            _tx.DataPager.MaybePrefetchMemory(_tx.DataPagerState,new TreePage.PagesReferencedEnumerator(page));
        }

        public bool MoveNext()
        {
            ThrowIfDisposedOnDebug(this, "TreeIterator " + _tree.Name);

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

        public bool Skip(long count)
        {
            if (count != 0)
            {
                var moveMethod = (count > 0) ? (Func<bool>)MoveNext : MovePrev;

                for (long i = 0; i < Math.Abs(count); i++)
                {
                    if (!moveMethod())
                        break;
                }
            }

            if (DoRequireValidation)
                return _currentPage != null && this.ValidateCurrentKey(_tx, Current);
            return _currentPage != null;
        }

        public ValueReader CreateReaderForCurrent()
        {
            return _tree.GetValueReaderFromHeader(Current);
        }

        public void Dispose()
        {
            // We may not want to execute `.Dispose()` more than once in release, but we want to fail fast in debug if it happens.
            ThrowIfDisposedOnDebug(this);
            if (_isDisposed)
                return;
            
            _isDisposed = true;
            
            if (RequiredPrefix.HasValue)
                RequiredPrefix.Release(_tx.Allocator);
            if (MaxKey.HasValue)
                MaxKey.Release(_tx.Allocator);
            _prevKeyScope.Dispose();
            _cursor?.Dispose();
            _decompressedPage?.Dispose();
            OnDisposal?.Invoke(this);

            // We want most operations to fail even if we are only checking disposed on debug. 
            _currentPage = null;
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

        private void DecompressedCurrentPage()
        {
            Debug.Assert(_tree.IsLeafCompressionSupported);

            _decompressedPage?.Dispose();

            _currentPage = _decompressedPage = _tree.DecompressPage(_currentPage, DecompressionUsage.Read, skipCache: false);
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
            using (TreeNodeHeader.ToSlicePtr(tx.Allocator, node, out Slice currentKey))
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
