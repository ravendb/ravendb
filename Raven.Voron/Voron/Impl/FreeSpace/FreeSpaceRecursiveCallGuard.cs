// -----------------------------------------------------------------------
//  <copyright file="FreeSpaceRecursiveCallGuard.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Voron.Impl.FreeSpace
{
    public class FreeSpaceRecursiveCallGuard : IDisposable
    {
        private readonly FreeSpaceHandling _freeSpaceHandling;
        public bool IsEntered;
        private Transaction _tx;
        public List<long> PagesFreed = new List<long>();

        public FreeSpaceRecursiveCallGuard(FreeSpaceHandling freeSpaceHandling)
        {
            _freeSpaceHandling = freeSpaceHandling;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Enter(Transaction tx)
        {
            if (IsEntered)
                throw new InvalidOperationException("Free space handling cannot be called recursively");

            IsEntered = true;
            _tx = tx;
            return this;
        }

        public void Dispose()
        {
            IsEntered = false;
            if (PagesFreed == null)
                return;
            var copy = PagesFreed;
            PagesFreed = null;
            foreach (var page in copy)
            {
                _freeSpaceHandling.FreePage(_tx, page);
            }
            _tx = null;
            copy.Clear();
            PagesFreed = copy;
        }
    }
}