using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Sparrow.Utils;

namespace Raven.Server.Extensions
{
    public static class TaskExtensions
    {
        public static async Task ThrowOnTimeout(this Task task, TimeSpan? timeout = null)
        {
            await InternalThrowOnTimeout(task, timeout ?? TimeSpan.FromSeconds(10));
            await task;
        }

        public static async Task<T> ThrowOnTimeout<T>(this Task<T> task, TimeSpan? timeout = null)
        {
            await InternalThrowOnTimeout(task, timeout ?? TimeSpan.FromSeconds(10));
            return await task;
        }

        private static async Task InternalThrowOnTimeout(Task task, TimeSpan timeout)
        {
            if (timeout.TotalMilliseconds <= 0) throw new ArgumentOutOfRangeException(nameof(timeout));
            var delay = TimeoutManager.WaitFor(timeout);
            if (await Task.WhenAny(task, delay) == delay)
            {
                throw new TimeoutException("Timeout exception while waiting for execution of the raft command to be completed.");
            }
        }
    }
}
