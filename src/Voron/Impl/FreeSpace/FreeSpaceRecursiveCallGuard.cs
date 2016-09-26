using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Voron.Impl.FreeSpace
{
    public class FreeSpaceRecursiveCallGuard : IDisposable
    {
        private readonly FreeSpaceHandling _freeSpaceHandling;
        public bool IsProcessingFixedSizeTree;
        private LowLevelTransaction _tx;
        public List<long> PagesFreed = new List<long>();

        public FreeSpaceRecursiveCallGuard(FreeSpaceHandling freeSpaceHandling)
        {
            _freeSpaceHandling = freeSpaceHandling;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Enter(LowLevelTransaction tx)
        {
            if (IsProcessingFixedSizeTree)
                throw new InvalidOperationException("Free space handling cannot be called recursively");

            IsProcessingFixedSizeTree = true;
            _tx = tx;
            return this;
        }

        public void Dispose()
        {
            IsProcessingFixedSizeTree = false;
            foreach (var page in PagesFreed)
            {
                _freeSpaceHandling.FreePage(_tx,page);
            }
            _tx = null;
            PagesFreed.Clear();

        }
    }
}