using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Sparrow.Utils;

namespace Raven.Server.Extensions
{
    public static class TaskExtensions
    {
        public static async Task ThrowOnTimeout(this Task task, int timeoutMs = 10_000)
        {
            await InternalThrowOnTimeout(task, timeoutMs);
            await task;
        }

        public static async Task<T> ThrowOnTimeout<T>(this Task<T> task, int timeoutMs = 10_000)
        {
            await InternalThrowOnTimeout(task, timeoutMs);
            return await task;
        }

        private static async Task InternalThrowOnTimeout(Task task, int timeoutMs)
        {
            if (timeoutMs <= 0) throw new ArgumentOutOfRangeException(nameof(timeoutMs));
            var delay = TimeoutManager.WaitFor(timeoutMs);
            if (await Task.WhenAny(task, delay) == delay)
            {
                throw new TimeoutException("Timeout exception while waiting for execution of the raft command to be completed.");
            }
        }
    }
}
