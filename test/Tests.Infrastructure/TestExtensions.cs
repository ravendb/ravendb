using System;
using System.Threading.Tasks;

namespace Tests.Infrastructure
{
    public static class TestExtensions
    {
        public static async Task<bool> WaitAsync(this Task task, TimeSpan timeout)
        {
            var delay = Task.Delay(timeout);
            var result = await Task.WhenAny(delay, task);
            return result == task;
        }

        public static async Task<bool> WaitAsync(this Task task, int timeout)
        {
            var delay = Task.Delay(timeout);
            var result = await Task.WhenAny(delay, task);
            return result == task;
        }
    }
}