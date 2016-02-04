using System.Collections.Concurrent;

namespace Raven.Abstractions.Extensions
{
    public static class ConcurrentQueueExtensions
    {
        public static void LimitedSizeEnqueue<T>(this ConcurrentQueue<T> queue, T item, int sizeOfQueue)
        {
            T dontCare;
            queue.Enqueue(item);
            while (queue.Count > sizeOfQueue)
                queue.TryDequeue(out dontCare);
        }
    }
}