using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Server.Extensions
{
    public static class WaitHandleExtensions
    {
        public static async Task<bool> WaitOneAsync(this WaitHandle handle, int millisecondsTimeout)
        {
            RegisteredWaitHandle registeredHandle = null;
            
            try
            {
                var tcs = new TaskCompletionSource<bool>();

                tcs.Task.PreventSynchronousContinuations();

                registeredHandle = ThreadPool.RegisterWaitForSingleObject(
                    handle,
                    (state, timedOut) => ((TaskCompletionSource<bool>)state).TrySetResult(timedOut == false),
                    tcs,
                    millisecondsTimeout,
                    true);

                return await tcs.Task;
            }
            finally
            {
                registeredHandle?.Unregister(null);
            }
        }

        public static Task<bool> WaitOneAsync(this WaitHandle handle, TimeSpan timeout)
        {
            return handle.WaitOneAsync((int)timeout.TotalMilliseconds);
        }

        public static Task<bool> WaitOneAsync(this WaitHandle handle)
        {
            return handle.WaitOneAsync(Timeout.Infinite);
        }
    }
}