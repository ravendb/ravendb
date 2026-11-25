using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Voron.Data.BTrees;
using Voron.Impl;

namespace Voron
{
    public sealed class TransactionPersistentContext(bool longLivedTransactions = false)
    {
        public bool LongLivedTransactions { get; set; } = longLivedTransactions;
        

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

        private readonly Stack<RecentlyFoundTreePages> _recentlyFoundPages = new();

        // the caches that the trees of the open transactions took, we take them back when their transaction is disposed
        private readonly List<RecentlyFoundTreePages> _lentRecentlyFoundPages = new();

        internal RecentlyFoundTreePages AllocateRecentlyFoundPages(Tree owner)
        {
            Debug.Assert(owner.Llt.IsDisposed == false, "A tree is used after its transaction was disposed");

            // cleared when it was returned, see FreeRecentlyFoundPages
            var pages = _recentlyFoundPages.TryPop(out var free) ? free : new RecentlyFoundTreePages();
            pages.Owner = owner;
            _lentRecentlyFoundPages.Add(pages);
            return pages;
        }

        /// <summary>
        /// Takes back the caches of the trees of a transaction that is disposed, the trees lose their reference to them.
        /// </summary>
        internal void FreeRecentlyFoundPages(LowLevelTransaction llt)
        {
            // the next transaction of an async commit may already use this context, its trees keep their caches
            int kept = 0;
            for (int i = 0; i < _lentRecentlyFoundPages.Count; i++)
            {
                var pages = _lentRecentlyFoundPages[i];
                if (pages.Owner.Llt != llt)
                {
                    _lentRecentlyFoundPages[kept++] = pages;
                    continue;
                }

                pages.Owner.DetachRecentlyFoundPages();
                pages.Owner = null;
                pages.Clear();
                if (_recentlyFoundPages.Count < 1024)
                    _recentlyFoundPages.Push(pages);
            }

            _lentRecentlyFoundPages.RemoveRange(kept, _lentRecentlyFoundPages.Count - kept);
        }
    }
}
