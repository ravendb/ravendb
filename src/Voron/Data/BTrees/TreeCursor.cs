using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sparrow;
using Sparrow.Collections;

namespace Voron.Data.BTrees
{
    public sealed class TreeCursor : IDisposable
    {
        private static readonly ObjectPool<FastStack<TreePage>> _treePageStackPool = new(() => new FastStack<TreePage>(16));

        public readonly FastStack<TreePage> _statePages = _treePageStackPool.Allocate();

        public FastStack<TreePage> Pages => _statePages;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // The bulk of the clean-up code is implemented in Dispose(bool)
        private void Dispose(bool disposing)
        {
            if (disposing == false) 
                return;

            _statePages.WeakClear();
            _treePageStackPool.Free(_statePages);
        }

        /// <summary>
        /// Replace the top of the cursor path with a new tree page. 
        /// </summary>
        public void SetTopPage(TreePage newVal)
        {
            ref var treePage = ref _statePages.TopByRef();
            treePage = newVal;
        }

        public TreePage ParentPage
        {
            get
            {
                if (_statePages.TryPeek(2, out TreePage result))
                    return result;

                throw new InvalidOperationException("No parent page in cursor");
            }
        }

        public TreePage CurrentPage => _statePages.Peek();

        public int PageCount => _statePages.Count;

        public void Push(TreePage p)
        {
            _statePages.Push(p);
        }

        public TreePage Pop()
        {
            if (_statePages.Count == 0)
                throw new InvalidOperationException("No page to pop");

            return _statePages.Pop();
        }
    }
}
