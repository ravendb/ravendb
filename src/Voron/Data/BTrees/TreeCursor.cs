using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sparrow;
using Sparrow.Collections;

namespace Voron.Data.BTrees
{
    public class TreeCursor : IDisposable
    {
        public FastStack<TreePage> Pages = new FastStack<TreePage>();

        private static readonly ObjectPool<FastDictionary<long, TreePage, NumericEqualityComparer>> _pagesByNumPool = new ObjectPool<FastDictionary<long, TreePage, NumericEqualityComparer>>(() => new FastDictionary<long, TreePage, NumericEqualityComparer>(50, default(NumericEqualityComparer)));

        private readonly FastDictionary<long, TreePage, NumericEqualityComparer> _pagesByNum;

        private bool _anyOverrides;

        public TreeCursor()
        {
            _pagesByNum = _pagesByNumPool.Allocate();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // The bulk of the clean-up code is implemented in Dispose(bool)
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _pagesByNum.Clear();
                _pagesByNumPool.Free(_pagesByNum);
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
                _pagesByNum[oldPageNumber] = newVal;
                return;
            }

            _anyOverrides = true;
            _pagesByNum[oldPageNumber] = newVal;
            _pagesByNum.Add(newPageNumber, newVal);
        }

        public TreePage ParentPage
        {
            get
            {
                TreePage result;
                if (Pages.TryPeek(2, out result))
                    return result;

                throw new InvalidOperationException("No parent page in cursor");
            }
        }

        public TreePage CurrentPage => Pages.Peek();

        public int PageCount => Pages.Count;

        public void Push(TreePage p)
        {
            Pages.Push(p);
            _pagesByNum.Add(p.PageNumber, p);
        }

        public TreePage Pop()
        {
            if (Pages.Count == 0)
                throw new InvalidOperationException("No page to pop");

            var p = Pages.Pop();

            var removedPrimary = _pagesByNum.Remove(p.PageNumber);
            var removedSecondary = false;

            if (_anyOverrides)
            {
                var pagesNumbersToRemove = new HashSet<long>();

                foreach (var page in _pagesByNum.Where(page => page.Value.PageNumber == p.PageNumber))
                    pagesNumbersToRemove.Add(page.Key);

                foreach (var pageToRemove in pagesNumbersToRemove)
                    removedSecondary |= _pagesByNum.Remove(pageToRemove);
            }

            Debug.Assert(removedPrimary || removedSecondary);

            return p;
        }
    }
}
