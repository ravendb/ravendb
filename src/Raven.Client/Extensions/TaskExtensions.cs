using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Extensions
{
    internal static class TaskExtensions
    {
        public static Task AssertNotFailed(this Task task)
        {
            if (task.IsFaulted)
                task.Wait(); // would throw

            return task;
        }

        public static Task WithCancellation(this Task task,
            CancellationToken token)
        {
            if (token == default(CancellationToken))
                return task;

            return task.ContinueWith(t => t.GetAwaiter().GetResult(), token);
        }

        public static Task<T> WithCancellation<T>(this Task<T> task,
            CancellationToken token)
        {
            if (token == default(CancellationToken))
                return task;

            return task.ContinueWith(t => t.ConfigureAwait(false).GetAwaiter().GetResult(), token);
        }

        public static async Task<bool> WaitWithTimeout(this Task task, TimeSpan? timeout)
        {
            if (timeout == null)
            {
                await task.ConfigureAwait(false);
                return true;
            }
            if (task == await Task.WhenAny(task, Task.Delay(timeout.Value)).ConfigureAwait(false))
                return true;
            return false;
        }

        public static Task<T> WithResult<T>(this Task task, T result)
        {
            return task.WithResult(() => result);
        }

        public static Task<T> WithResult<T>(this Task task, Func<T> result)
        {
            return task.ContinueWith(t =>
            {
                t.AssertNotFailed();
                return result();
            });
        }

        public static Task<T> WithResult<T>(this Task task, Task<T> result)
        {
            return task.WithResult<Task<T>>(result).Unwrap();
        }

        public static Task<T> ContinueWithTask<T>(this Task task, Func<Task<T>> result)
        {
            return task.WithResult(result).Unwrap();
        }

        public static Task ContinueWithTask(this Task task, Func<Task> result)
        {
            return task.WithResult(result).Unwrap();
        }
    }
}
