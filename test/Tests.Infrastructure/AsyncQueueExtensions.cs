using System;
using System.Threading.Tasks;
using Sparrow.Server.Collections;

namespace Tests.Infrastructure;

public static class AsyncQueueExtensions
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(1);
    
    public static async Task<T> DequeueUntilAsync<T>(this AsyncQueue<T> queue, Predicate<T> predicate, TimeSpan? maxWaitForDequeue = null)
    {
        // For the first, wait asynchronously with no timeout.
        // The reason to wait with no timeout here is to wait for some setup that might take more than timeout.
        // For example notifications, if the setup takes 2s, no item will be found and it will break the test.
        // The first timeout-less wait ensure that there's always enough of time to set things up.
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
