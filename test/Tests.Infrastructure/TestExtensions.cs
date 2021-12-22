using System;
using System.Threading.Tasks;

namespace Tests.Infrastructure
{
    public static class TestExtensions
    {
        public static async Task<bool> WaitWithoutExceptionAsync(this Task task, TimeSpan timeout)
        {
            return await WaitWithoutExceptionAsync(task, (int)timeout.TotalMilliseconds).ConfigureAwait(false);
        }

        public static async Task<bool> WaitWithoutExceptionAsync(this Task task, int timeout)
        {
            var delay = Task.Delay(Math.Max(timeout, 1000));
            var result = await Task.WhenAny(task, delay).ConfigureAwait(false);
            if (result == delay && task.IsCompleted == false)
                return false;
            if (task.IsCompletedSuccessfully)
                return true;
            if (task.Exception != null)
                await task;
            throw new Exception($"Should never reach this code path. {task.Status}, timeout: {timeout}");
        }
    }
}
