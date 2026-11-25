using System;
using System.Runtime.CompilerServices;

namespace Voron.Data.BTrees
{
    /// <summary>
    /// The pages on the path from the root to the current page, the current page on top.
    /// </summary>
    public sealed class TreeCursor : ITreeDescentPath
    {
        // trees with keys below 1KB are at most 8 levels deep, a deeper tree moves the path to _deepPath
        private const int InlineDepth = 8;

        [InlineArray(InlineDepth)]
        private struct InlinePath
        {
            private TreePage _element;
        }

        private InlinePath _inlinePath;
        private TreePage[] _deepPath;
        private int _count;

        private Span<TreePage> Path
        {
            get
            {
                if (_deepPath != null)
                    return _deepPath;
                return _inlinePath;
            }
        }

        public int PageCount => _count;

        // on an empty cursor the index is negative, and the span throws
        public TreePage CurrentPage => Path[_count - 1];

        public TreePage ParentPage => Path[_count - 2];

        unsafe void ITreeDescentPath.PushBranch(TreePage branch)
        {
            // we copy, beause the branch we are passed is reused by the caller
            Push(new TreePage(branch.Base, branch.PageSize)
            {
                LastSearchPosition = branch.LastSearchPosition,
                LastMatch = branch.LastMatch
            });
        }

        public void Push(TreePage p)
        {
            if (_count == Path.Length)
                Grow();

            Path[_count++] = p;
        }

        public TreePage Pop()
        {
            var path = Path;
            var p = path[_count - 1]; // throws before _count changes
            path[--_count] = null;
            return p;
        }

        /// <summary>
        /// Replaces the current page, e.g. with its writable copy.
        /// </summary>
        public void Update(TreePage page)
        {
            Path[_count - 1] = page;
        }

        public void Clear()
        {
            Path[.._count].Clear();
            _count = 0;
        }

        /// <summary>
        /// Drops the page and the pages above it, false when the page is not on the cursor.
        /// </summary>
        public bool TruncateAt(long pageNumber)
        {
            var path = Path[.._count];
            for (int i = 0; i < path.Length; i++)
            {
                if (path[i].PageNumber != pageNumber)
                    continue;

                path[i..].Clear();
                _count = i;
                return true;
            }

            return false;
        }

        private void Grow()
        {
            var deeper = new TreePage[Path.Length * 2];
            Path.CopyTo(deeper);
            _deepPath = deeper;
        }
    }
}
