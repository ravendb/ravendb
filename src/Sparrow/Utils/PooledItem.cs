using System;

namespace Sparrow.Utils
{
    public abstract class PooledItem : IDisposable
    {
        public int InUse;
        public DateTime InPoolSince;

        public abstract void Dispose();
    }
}