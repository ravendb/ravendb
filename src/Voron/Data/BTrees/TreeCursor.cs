using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    /// <summary>
    /// The path of pages from the root to the current leaf. The descent records only page numbers
    /// (free - it visits them anyway); the actual page entries are materialized on the first
    /// structural use, since most operations never split or rebalance. Everything lives inline in
    /// the struct, so a cursor costs no allocation unless the tree is deeper than
    /// <see cref="FoundTreePageDescriptor.MaxCursorPath"/>, which our fan-out makes unreachable
    /// in practice.
    ///
    /// WARNING: a <c>ref TreePage</c> obtained from <see cref="CurrentPageRef"/> is invalidated by
    /// any Push/Pop - re-acquire it after structural changes.
    /// </summary>
    public unsafe struct TreeCursor
    {
        [InlineArray(FoundTreePageDescriptor.MaxCursorPath)]
        private struct InlinePages
        {
            private TreePage _element0;
        }

        private Tree _tree;
        private LowLevelTransaction _llt;
        private Slice _key;
        private fixed long _path[FoundTreePageDescriptor.MaxCursorPath];
        private long[] _overflowPath;
        private TreePage _leaf;
        private int _pathLength;
        private bool _materialized;

        private InlinePages _pages;
        private TreePage[] _overflowPages;
        private int _count;

        /// <summary>
        /// Filled by the descent: the ancestor page numbers plus a copy of the leaf carrying its
        /// search position. Cheap enough to produce on every write operation.
        /// </summary>
        public TreeCursor(LowLevelTransaction llt, Tree tree, Slice key, TreePage leaf, ReadOnlySpan<long> path)
        {
            _llt = llt;
            _tree = tree;
            _key = key;
            _leaf = leaf;
            _pathLength = path.Length;

            if (path.Length <= FoundTreePageDescriptor.MaxCursorPath)
            {
                path.CopyTo(MemoryMarshal.CreateSpan(ref _path[0], FoundTreePageDescriptor.MaxCursorPath));
            }
            else
            {
                _overflowPath = path.ToArray();
            }
        }

        private void EnsureMaterialized()
        {
            if (_materialized)
                return;

            _materialized = true;

            ReadOnlySpan<long> path = _overflowPath != null
                ? _overflowPath.AsSpan(0, _pathLength)
                : MemoryMarshal.CreateReadOnlySpan(ref _path[0], _pathLength);

            foreach (var pageNumber in path)
            {
                if (pageNumber == _leaf.PageNumber)
                {
                    PushCore(_leaf);
                    continue;
                }

                var page = _tree.GetReadOnlyTreePage(pageNumber);
                if (_key.Options == SliceOptions.Key)
                {
                    page.Search(_llt, _key);
                    if (page.LastMatch != 0)
                        page.LastSearchPosition--;
                }
                else if (_key.Options == SliceOptions.BeforeAllKeys)
                {
                    page.LastSearchPosition = 0;
                }
                else if (_key.Options == SliceOptions.AfterAllKeys)
                {
                    page.LastSearchPosition = (short)(page.NumberOfEntries - 1);
                }
                else
                {
                    throw new ArgumentException("Invalid key option: " + _key.Options);
                }

                PushCore(page);
            }
        }

        private void PushCore(TreePage p)
        {
            if (_count < FoundTreePageDescriptor.MaxCursorPath)
            {
                _pages[_count++] = p;
                return;
            }

            GrowUnlikely(p);
        }

        private void GrowUnlikely(TreePage p)
        {
            _overflowPages ??= new TreePage[FoundTreePageDescriptor.MaxCursorPath * 2];
            if (_count - FoundTreePageDescriptor.MaxCursorPath >= _overflowPages.Length)
                Array.Resize(ref _overflowPages, _overflowPages.Length * 2);

            _overflowPages[_count - FoundTreePageDescriptor.MaxCursorPath] = p;
            _count++;
        }

        [UnscopedRef]
        private ref TreePage Slot(int index)
        {
            if (index < FoundTreePageDescriptor.MaxCursorPath)
                return ref _pages[index];

            return ref _overflowPages[index - FoundTreePageDescriptor.MaxCursorPath];
        }

        public int PageCount
        {
            get
            {
                EnsureMaterialized();
                return _count;
            }
        }

        public TreePage CurrentPage
        {
            get
            {
                EnsureMaterialized();
                return Slot(_count - 1);
            }
        }

        [UnscopedRef]
        public ref TreePage CurrentPageRef
        {
            get
            {
                EnsureMaterialized();
                return ref Slot(_count - 1);
            }
        }

        public TreePage ParentPage
        {
            get
            {
                EnsureMaterialized();
                if (_count < 2)
                    throw new InvalidOperationException("No parent page in cursor");

                return Slot(_count - 2);
            }
        }

        public void SyncTopPage(TreePage page)
        {
            EnsureMaterialized();
            if (_count == 0)
                return;

            ref var top = ref Slot(_count - 1);
            if (top.PageNumber == page.PageNumber)
                top = page;
        }

        /// <summary>
        /// Replace the top of the cursor path with a new tree page.
        /// </summary>
        public void SetTopPage(TreePage newVal)
        {
            EnsureMaterialized();
            Slot(_count - 1) = newVal;
        }

        public void Push(TreePage p)
        {
            EnsureMaterialized();
            PushCore(p);
        }

        public TreePage Pop()
        {
            EnsureMaterialized();
            if (_count == 0)
                throw new InvalidOperationException("No page to pop");

            var top = Slot(_count - 1);
            _count--;
            return top;
        }
    }
}
