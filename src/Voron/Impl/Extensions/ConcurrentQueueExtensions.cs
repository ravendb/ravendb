namespace Voron.Impl.Extensions
{
    using System.Collections.Concurrent;

    public static class ConcurrentQueueExtensions
    {
        public static T Peek<T>(this ConcurrentQueue<T> self)
            where T : class
        {
            T result;
            if (self.TryPeek(out result) == false)
                return null;
            return result;
        }

        // This function does not ensure thread-safty, so the size will not be exact. But it bound the queue without locking. 
        public static ConcurrentQueue<T> Reduce<T>(this ConcurrentQueue<T> queue, int size)
        {
            var canDequeu = true;
            while (canDequeu && queue.Count > size)
            {
                canDequeu = queue.TryDequeue(out var el);
            }
            return queue;
        }
    }
}
