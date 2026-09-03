using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sparrow;
using Sparrow.Collections;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    public sealed class TreeCursor : IDisposable
    {
        private readonly TransactionPersistentContext _context;

        public readonly FastStack<TreePage> _statePages;

        public TreeCursor(LowLevelTransaction llt)
        {
            _context = llt.PersistentContext;
            _statePages = _context.AllocateCursorPages();
        }

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
            _context.FreeCursorPages(_statePages);
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

        /// <summary>
        /// The top page by reference. TreePage is a value type, so a caller that needs its search
        /// state to be visible through the cursor has to work against the slot, not against a copy.
        /// </summary>
        public ref TreePage CurrentPageRef => ref _statePages.TopByRef();

        /// <summary>
        /// Writes back a page that the cursor already holds. TreePage is a value type, so search
        /// state that a caller sets on its own copy is not visible through the cursor until it is
        /// synced back.
        /// </summary>
        public void SyncTopPage(TreePage page)
        {
            if (_statePages.Count == 0)
                return;

            ref var top = ref _statePages.TopByRef();
            if (top.PageNumber == page.PageNumber)
                top = page;
        }

        /// <summary>
        /// Pushes a page and hands back the slot it now occupies. Any further push invalidates the
        /// returned reference, so callers re-acquire it after each push.
        /// </summary>
        public ref TreePage PushAndGetRef(TreePage p)
        {
            _statePages.Push(p);
            return ref _statePages.TopByRef();
        }

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
