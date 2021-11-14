using System;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Utils;

namespace Raven.Client.Extensions
{
    public static class TaskExtensions
    {
        public static Task IgnoreUnobservedExceptions(this Task task)
        {
            return task.ContinueWith(t => GC.KeepAlive(t.Exception), TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted);
        }

        public static Task AssertNotFailed(this Task task)
        {
            if (task.IsFaulted)
                task.Wait(); // would throw

            return task;
        }

        public static Task WithCancellation(this Task task,
            CancellationToken token)
        {
            return token == default(CancellationToken) ? 
                task : 
                task.ContinueWith(t => t.ConfigureAwait(false).GetAwaiter().GetResult(), token);
        }

        public static Task<T> WithCancellation<T>(this Task<T> task,
            CancellationToken token)
        {
            return token == default(CancellationToken) ? 
                task : 
                task.ContinueWith(t => t.ConfigureAwait(false).GetAwaiter().GetResult(), token);
        }

        public static async Task<bool> WaitWithTimeout(this Task task, TimeSpan? timeout)
        {
            if (timeout == null)
            {
                await task.ConfigureAwait(false);
                return true;
            }

            return task == await Task.WhenAny(task, TimeoutManager.WaitFor(timeout.Value)).ConfigureAwait(false);
        }

        internal static async Task WaitAndThrowOnTimeout(this Task task, TimeSpan timeout)
        {
            var result = await Task.WhenAny(task, TimeoutManager.WaitFor(timeout)).ConfigureAwait(false);
                
            if (result != task)
                throw new TimeoutException($"Task wasn't completed within {timeout}.");

            await result.ConfigureAwait(false);
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
