using System;
using Sparrow.Threading;

namespace Sparrow.Utils
{
    public abstract class PooledItem : IDisposable
    {
        public MultipleUseFlag InUse = new MultipleUseFlag();
        public DateTime InPoolSince;

        public abstract void Dispose();
    }
}
