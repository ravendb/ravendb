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
        private readonly Stack<PageLocator> pageLocators = new Stack<PageLocator>();
        private readonly ByteStringContext _allocator = new ByteStringContext();

        public TransactionPersistentContext(bool isLongLived = false)
        {
            IsLongLived = isLongLived;
        }

        public PageLocator AllocatePageLocator(LowLevelTransaction tx)
        {
            PageLocator locator = null;

            if (pageLocators.Count != 0)
            {
                locator = pageLocators.Pop();
                locator.Renew(tx, IsLongLived ? 128 : 16);
            }
            else
            {
                locator = new PageLocator(_allocator, tx, IsLongLived ? 128 : 16);
            }

            return locator;

        }

        public void FreePageLocator(PageLocator locator)
        {
            Debug.Assert(locator != null);
            pageLocators.Push(locator);
        }
    }
}