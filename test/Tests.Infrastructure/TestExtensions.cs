using System;
using System.Threading.Tasks;

namespace Tests.Infrastructure
{
    public static class TestExtensions
    {
        public static async Task<bool> WaitAsync(this Task task, TimeSpan timeout)
        {
            return await WaitAsync(task, (int)timeout.TotalMilliseconds).ConfigureAwait(false);
        }

        public static async Task<bool> WaitAsync(this Task task, int timeout)
        {
            var delay = Task.Delay(Math.Max(timeout, 1000));
            var result = await Task.WhenAny(task, delay).ConfigureAwait(false);
            if (result == delay && task.IsCompleted == false)
                return false;
            if (task.IsCompletedSuccessfully)
                return true;
            if (task.Exception != null)
                throw task.Exception.GetBaseException();
            throw new Exception($"Should never reach this code path. {task.Status}, timeout: {timeout}");
        }
    }
}
