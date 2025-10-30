using System;
using System.Threading.Tasks;
using Sparrow.Server.Collections;

namespace Tests.Infrastructure;

public static class AsyncQueueExtensions
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(1);
    
    public static async Task<T> DequeueUntilAsync<T>(this AsyncQueue<T> queue, Predicate<T> predicate, TimeSpan? maxWaitForDequeue = null)
    {
        // For the first, wait asynchronously with no timeout
        T first = await queue.DequeueAsync();
        if (predicate(first))
            return first;
        
        while (true)
        {
            (bool success, T item) = await queue.TryDequeueAsync(maxWaitForDequeue ?? DefaultTimeout);
            if (success)
            {
                if (predicate(item))
                {
                    return item;
                }
            }

            else
            {
                throw new Exception("No matching item found");        
            }
        }
    }
}
