using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Voron.Impl;

namespace Voron
{
    public sealed class TransactionPersistentContext
    {
        private bool _longLivedTransaction;

        public bool LongLivedTransactions
        {
            get { return _longLivedTransaction; }
            set
            {
                _longLivedTransaction = value;
            }
        }

        private readonly Stack<PageLocator> _pageLocators = new Stack<PageLocator>();

        public TransactionPersistentContext(bool longLivedTransactions = false)
        {
            LongLivedTransactions = longLivedTransactions;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PageLocator AllocatePageLocator(LowLevelTransaction tx)
        {
            PageLocator locator;
            if (_pageLocators.Count != 0)
            {
                locator = _pageLocators.Pop();
                locator.Renew(tx);
            }
            else
            {
                locator = new PageLocator(tx);
            }
            return locator;
        }

        public void FreePageLocator(PageLocator locator)
        {
            Debug.Assert(locator != null);
            locator.Release();
            if (_pageLocators.Count < 1024)
                _pageLocators.Push(locator);
        }

    }
}
