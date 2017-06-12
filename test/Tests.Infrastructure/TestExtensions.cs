using System;
using System.Threading.Tasks;

namespace Tests.Infrastructure
{
    public static class TestExtensions
    {
        public static async Task<bool> WaitAsync(this Task task, TimeSpan timeout)
        {
            var delay = Task.Delay(timeout);
            await Task.WhenAny(delay, task);
            return task.IsCompleted;
        }

        public static async Task<bool> WaitAsync(this Task task, int timeout)
        {
            var delay = Task.Delay(timeout);
            await Task.WhenAny(delay, task);
            return task.IsCompleted;
        }
    }
}