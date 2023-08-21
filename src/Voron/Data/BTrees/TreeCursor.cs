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
        private sealed class TreeCursorState
        {
            public readonly Dictionary<long, TreePage> PageByNum;
            public readonly FastStack<TreePage> Pages;
            
            public TreeCursorState(int pageCapacity, int stackCapacity)
            {
                PageByNum = new(pageCapacity);
                Pages = new(stackCapacity);
            }
            
            public void Clear()
            {
                PageByNum.Clear();
                Pages.Clear();
            }
        }
        
        private static readonly ObjectPool<TreeCursorState> _pagesByNumPool = new ObjectPool<TreeCursorState>(() => new TreeCursorState(50, 16));
        private bool _anyOverrides;
        private readonly TreeCursorState _state;
        public FastStack<TreePage> Pages => _state.Pages;

        public TreeCursor()
        {
            _state = _pagesByNumPool.Allocate();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // The bulk of the clean-up code is implemented in Dispose(bool)
        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                _state.Clear();
                _pagesByNumPool.Free(_state);
            }
        }

        public void Update(FastStack<TreePage> stack, TreePage newVal)
        {
            var oldNode = stack.Pop();
            stack.Push(newVal);

            var oldPageNumber = oldNode.PageNumber;
            var newPageNumber = newVal.PageNumber;

            if (oldPageNumber == newPageNumber)
            {
                _state.PageByNum[oldPageNumber] = newVal;
                return;
            }

            _anyOverrides = true;
            _state.PageByNum[oldPageNumber] = newVal;
            _state.PageByNum.Add(newPageNumber, newVal);
        }

        public TreePage ParentPage
        {
            get
            {
                TreePage result;
                if (_state.Pages.TryPeek(2, out result))
                    return result;

                throw new InvalidOperationException("No parent page in cursor");
            }
        }

        public TreePage CurrentPage => _state.Pages.Peek();

        public int PageCount => _state.Pages.Count;

        public void Push(TreePage p)
        {
            _state.Pages.Push(p);
            _state.PageByNum.Add(p.PageNumber, p);
        }

        public TreePage Pop()
        {
            if (_state.Pages.Count == 0)
                throw new InvalidOperationException("No page to pop");

            var p = _state.Pages.Pop();

            var removedPrimary = _state.PageByNum.Remove(p.PageNumber);
            var removedSecondary = false;

            if (_anyOverrides)
            {
                var pagesNumbersToRemove = new HashSet<long>();

                foreach (var page in _state.PageByNum.Where(page => page.Value.PageNumber == p.PageNumber))
                    pagesNumbersToRemove.Add(page.Key);

                foreach (var pageToRemove in pagesNumbersToRemove)
                    removedSecondary |= _state.PageByNum.Remove(pageToRemove);
            }

            Debug.Assert(removedPrimary || removedSecondary);

            return p;
        }
    }
}
