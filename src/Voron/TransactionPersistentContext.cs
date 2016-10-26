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
                locator.Renew(tx, _cacheSize);
            }
            else
            {
                locator = new PageLocator(tx, _cacheSize);
            }
            return locator;

        }

        public void FreePageLocator(PageLocator locator)
        {
            Debug.Assert(locator != null);
            _pageLocators.Push(locator);
        }

        public void Dispose()
        {
            foreach (var pageLocator in _pageLocators)
            {
                pageLocator.Dispose();
            }
        }
    }
}