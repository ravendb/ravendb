using System;
using System.Runtime.CompilerServices;

namespace Voron.Impl.FreeSpace
{
    public class FreeSpaceRecursiveCallGuard : IDisposable
    {
        private bool _isProcessingFixedSizeTree;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Enter()
        {
            if (_isProcessingFixedSizeTree)
                throw new InvalidOperationException("Free space handling cannot be called recursively");

            _isProcessingFixedSizeTree = true;

            return this;
        }

        public void Dispose()
        {
            _isProcessingFixedSizeTree = false;
        }
    }
}