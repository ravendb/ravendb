using System;

namespace Sparrow.Utils
{
    public abstract class PooledItem : IDisposable
    {
        // TODO: Replace for a SingleUseFlag STRUCT.
        public int InUse;
        public DateTime InPoolSince;

        public abstract void Dispose();
    }
}
