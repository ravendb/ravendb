﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Json;
using Voron.Impl;

namespace Voron
{
    public class TransactionPersistentContext : IDisposable
    {
        private bool _longLivedTransaction;
        private int _cacheSize;

        public bool LongLivedTransactions
        {
            get { return _longLivedTransaction; }
            set
            {
                _longLivedTransaction = value;
                _cacheSize = _longLivedTransaction ? 128 : 16;
            }
        }

        private readonly Stack<PageLocator> pageLocators = new Stack<PageLocator>();
        private readonly ByteStringContext _allocator = new ByteStringContext();

        public TransactionPersistentContext(bool longLivedTransactions = false)
        {
            LongLivedTransactions = longLivedTransactions;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PageLocator AllocatePageLocator(LowLevelTransaction tx)
        {
            PageLocator locator = null;
            if (pageLocators.Count != 0)
            {
                locator = pageLocators.Pop();
                locator.Renew(tx, _cacheSize);
            }
            else
            {
                locator = new PageLocator(_allocator, tx, _cacheSize);
            }

            return locator;

        }

        public void FreePageLocator(PageLocator locator)
        {
            Debug.Assert(locator != null);
            pageLocators.Push(locator);
        }

        public void Dispose()
        {
            _allocator?.Dispose();
        }
    }
}