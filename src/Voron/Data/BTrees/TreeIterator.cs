using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Server;
using Voron.Data.Compression;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    public sealed unsafe class TreeIterator : IIterator, ITreeDescentPath
    {
        private readonly Tree _tree;
        private readonly LowLevelTransaction _tx;
        private bool _prefetch;

        private struct PathEntry
        {
            public byte* Base;
            public int LastSearchPosition;
        }

        // trees with keys below 1KB are at most 8 levels deep, a deeper tree moves the path to _deepPath
        [InlineArray(8)]
        private struct InlinePath
        {
            private PathEntry _element;
        }

        private InlinePath _inlinePath;
        private PathEntry[] _deepPath;
        private int _pathDepth;

        // the iterator's own instance, pointed at the page it is on: the leaf, or a branch while it moves between leaves
        private readonly TreePage _page = new(null, Constants.Storage.PageSize);

        // _page, or _decompressedPage on a compressed leaf
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

        internal bool IsDisposed => _disposed;

        internal void Reuse(bool prefetch)
        {
            Debug.Assert(_disposed);

            _disposed = false;
            _prefetch = prefetch;
            _currentKey = default;
            _currentInternalKey = default;
            _prevKeyScope = default;
            _requiredPrefix = default;
            _maxKey = default;
            _requireValidation = false;
        }

        private Span<PathEntry> Path
        {
            get
            {
                if (_deepPath != null)
                    return _deepPath;
                return _inlinePath;
            }
        }

        void ITreeDescentPath.PushBranch(TreePage branch) => PushBranch(branch);

        private void PushBranch(TreePage branch)
        {
            if (_pathDepth == Path.Length)
            {
                var deeper = new PathEntry[Path.Length * 2];
                Path.CopyTo(deeper);
                _deepPath = deeper;
            }

            ref var entry = ref Path[_pathDepth++];
            entry.Base = branch.Base;
            entry.LastSearchPosition = branch.LastSearchPosition;
        }

        // moves the iterator up to the branch above the current page
        private void PopBranch()
        {
            ref var entry = ref Path[_pathDepth - 1];
            _pathDepth--;

            _currentPage = MoveTo(entry.Base);
            _currentPage.LastSearchPosition = entry.LastSearchPosition;
        }

        private TreePage MoveTo(byte* pagePointer)
        {
            _page.Base = pagePointer;
            return _page;
        }

        public int GetCurrentDataSize()
        {
            if (_disposed)
                throw new ObjectDisposedException("TreeIterator " + _tree.Name);
            return _tree.GetDataSize(Current);
        }

        public bool SeekBackward(Slice key)
        {
            return SeekInternal(key, backward: true);
        }

        public bool Seek(Slice key)
        {
            return SeekInternal(key, backward: false);
        }

        private bool SeekInternal(Slice key, bool backward)
        {
            if (_disposed)
                throw new ObjectDisposedException("TreeIterator " + _tree.Name);

            _pathDepth = 0;
            _currentPage = _tree.FindPageFor(key, this, _page, out TreeNodeHeader* node, allowCompressed: _tree.IsLeafCompressionSupported, backward: backward);
            if (_currentPage.IsCompressed)
            {
                DecompressedCurrentPage();
                node = _currentPage.Search(_tx, key, backward: backward);
            }

            if (node != null)
            {
                _prevKeyScope.Dispose();
                _prevKeyScope = TreeNodeHeader.ToSlicePtr(_tx.Allocator, node, out _currentInternalKey);
                _currentKey = _currentInternalKey;

                if (DoRequireValidation)
                    return this.ValidateCurrentKey(_tx, Current);
                return true;
            }

            if (backward)
            {
                // We know that the exact value isn't there, but it is possible that the previous page has values 
                // that is actually less than the key, so we need to check it as well.
                _currentPage.LastSearchPosition = -1; // force next MovePrev to move to the previous _page_.
                return MovePrev();
            }
            else
            {
                // We know that the exact value isn't there, but it is possible that the next page has values 
                // that is actually greater than the key, so we need to check it as well.
                _currentPage.LastSearchPosition = _currentPage.NumberOfEntries; // force next MoveNext to move to the next _page_.
                return MoveNext();
            }
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

                        PushBranch(_currentPage);
                        var node = _currentPage.GetNode(_currentPage.LastSearchPosition);
                        _currentPage = MoveTo(_tx.GetPage(node->PageNumber).Pointer);

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
                if (_pathDepth == 0)
                    break;
                PopBranch();
            }
            _currentPage = null;
            return false;
        }

        private void MaybePrefetchPagesReferencedBy(TreePage page)
        {
            _tx.DataPager.MaybePrefetchMemory(new TreePage.PagesReferencedEnumerator(page));
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

                        PushBranch(_currentPage);
                        var node = _currentPage.GetNode(_currentPage.LastSearchPosition);
                        _currentPage = MoveTo(_tx.GetPage(node->PageNumber).Pointer);

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
                if (_pathDepth == 0)
                    break;
                PopBranch();
            }
            _currentPage = null;

            return false;
        }

        internal long CountToEndOfRequiredPrefix()
        {
            Debug.Assert(_requiredPrefix.HasValue && _maxKey.HasValue == false);

            long count = 0;
            byte* lastLeaf = null; // the leaf where the prefix ends
            while (true)
            {
                var page = _currentPage;
                var lastPosition = page.NumberOfEntries - 1;
                if (page.Base != lastLeaf && page.LastSearchPosition < lastPosition)
                {
                    using (TreeNodeHeader.ToSlicePtr(_tx.Allocator, page.GetNode(lastPosition), out Slice lastKey))
                    {
                        if (SliceComparer.StartWith(lastKey, _requiredPrefix))
                        {
                            count += lastPosition - page.LastSearchPosition;
                            page.LastSearchPosition = lastPosition;
                        }
                        else
                        {
                            lastLeaf = page.Base;
                        }
                    }
                }

                count++;
                if (MoveNext() == false)
                    return count;
            }
        }

        public bool Skip(long count)
        {
            for (long i = 0; i < Math.Abs(count); i++)
            {
                if ((count > 0 ? MoveNext() : MovePrev()) == false)
                    break;
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
            if (_disposed)
                return;
            _disposed = true;
            if (RequiredPrefix.HasValue)
                RequiredPrefix.Release(_tx.Allocator);
            if (MaxKey.HasValue)
                MaxKey.Release(_tx.Allocator);
            _prevKeyScope.Dispose();
            _decompressedPage?.Dispose();
            _decompressedPage = null;
            _currentPage = null;
            OnDisposal?.Invoke(this);
            OnDisposal = null; // the next user of this iterator subscribes again
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

            // the decompressed page keeps the page it was made from, and _page moves on, so it gets a copy
            var original = new TreePage(_currentPage.Base, _currentPage.PageSize);
            _currentPage = _decompressedPage = _tree.DecompressPage(original, DecompressionUsage.Read, skipCache: false);
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
