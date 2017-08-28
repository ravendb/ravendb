using System.Collections.Concurrent;

namespace Raven.Server.Utils
{
    public static class ConcurrentQueueExtensions
    {
        public static void LimitedSizeEnqueue<T>(this ConcurrentQueue<T> queue, T item, int sizeOfQueue)
        {
            queue.Enqueue(item);
            while (queue.Count > sizeOfQueue)
                queue.TryDequeue(out T _);
        }
    }
}
