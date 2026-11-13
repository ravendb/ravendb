using System;
using Sparrow.Threading;

namespace Sparrow.Utils
{
    public abstract class PooledItem : IDisposable
    {
        public MultipleUseFlag InUse = new MultipleUseFlag();

        /// <summary>
        /// <see cref="Clock.Milliseconds"/> when the item was returned to its pool.
        /// </summary>
        public long InPoolSinceMilliseconds;

        public abstract void Dispose();
    }
}
