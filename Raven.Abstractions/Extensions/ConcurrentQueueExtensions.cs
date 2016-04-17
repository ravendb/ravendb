using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Extensions
{
    public static class ConcurrentQueueExtensions
    {
        public static void LimitedSizeEnqueue<T>(this ConcurrentQueue<T> queue,T item, int sizeOfQueue)
        {
            T dontCare;
            queue.Enqueue(item);
            while (queue.Count > sizeOfQueue)
                queue.TryDequeue(out dontCare);
        }
    }
}
