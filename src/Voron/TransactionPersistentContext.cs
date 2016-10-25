using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow;
using Sparrow.Json;
using Voron.Impl;

namespace Voron
{
    public class TransactionPersistentContext
    {
        public bool IsLongLived { get; set; }
        private readonly Stack<PageLocator> _pageLocators = new Stack<PageLocator>();
        private readonly ByteStringContext _allocator = new ByteStringContext();

        public TransactionPersistentContext(bool isLongLived = false)
        {
            IsLongLived = isLongLived;
        }

        public PageLocator AllocatePageLocator(LowLevelTransaction tx)
        {
            PageLocator locator = null;

            if (_pageLocators.Count != 0)
            {
                locator = _pageLocators.Pop();
                locator.Renew(tx, IsLongLived ? 128 : 16);
            }
            else
            {
                locator = new PageLocator(tx, IsLongLived ? 128 : 16);
            }

            return locator;

        }

        public void FreePageLocator(PageLocator locator)
        {
            Debug.Assert(locator != null);
            _pageLocators.Push(locator);
        }
    }
}