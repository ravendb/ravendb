using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Collections;
using Voron.Data.BTrees;
using Voron.Impl;

namespace Voron
{
    public sealed class TransactionPersistentContext(bool longLivedTransactions = false)
    {
        public bool LongLivedTransactions { get; set; } = longLivedTransactions;
        

         // async commit keeps multuple transactions alive at once over the same context, so we use a stack for those
        private readonly Stack<FastStack<TreePage>> _cursorPages = new();
        private readonly Stack<PageLocator> _pageLocators = new();
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PageLocator AllocatePageLocator()
        {
            PageLocator locator;
            if (_pageLocators.Count != 0)
            {
                locator = _pageLocators.Pop();
                locator.Renew();
            }
            else
            {
                locator = new PageLocator();
            }
            return locator;
        }

        internal void FreePageLocator(PageLocator locator)
        {
            Debug.Assert(locator != null);
            if (_pageLocators.Count < 1024)
                _pageLocators.Push(locator);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal FastStack<TreePage> AllocateCursorPages()
        {
            return _cursorPages.Count != 0 ? _cursorPages.Pop() : new FastStack<TreePage>(16);
        }

        internal void FreeCursorPages(FastStack<TreePage> pages)
        {
            Debug.Assert(pages != null);
            if (_cursorPages.Count < 64)
                _cursorPages.Push(pages);
        }
    }
}
