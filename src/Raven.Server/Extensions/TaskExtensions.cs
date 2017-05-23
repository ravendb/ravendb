using System;
using System.Threading.Tasks;

namespace Raven.Server.Extensions
{
    public static class TaskExtensions
    {
        public static async Task ThrowOnTimeout(this Task task, int timeoutMs = 10_000)
        {
            await InternalThrowOnTimeout(task, timeoutMs);
        }

        public static async Task<T> ThrowOnTimeout<T>(this Task<T> task, int timeoutMs = 10_000)
        {
            await InternalThrowOnTimeout(task, timeoutMs);
            return task.Result;
        }

        private static async Task InternalThrowOnTimeout(Task task, int timeoutMs)
        {
            var delay = Task.Delay(timeoutMs);
            if (await Task.WhenAny(task, delay) == delay)
            {
                throw new TimeoutException("Timeout exception while waiting for execution of the raft command to be completed.");
            }
        }
    }
}
