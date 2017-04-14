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

        private const int TASK_STATE_THREAD_WAS_ABORTED = 134217728;
        private const string stateFlagsFieldName = "m_stateFlags";
        private static readonly ParameterExpression TaskParameter = Expression.Parameter(typeof(Task));

        // http://stackoverflow.com/questions/22579206/how-can-i-prevent-synchronous-continuations-on-a-task
        private static readonly Action<Task> EnsureContinuationsWontBeCalledSynchronously = Expression.Lambda<Action<Task>>(
            Expression.Assign(Expression.Field(TaskParameter, stateFlagsFieldName),
                Expression.Or(Expression.Field(TaskParameter, stateFlagsFieldName),
                    Expression.Constant(TASK_STATE_THREAD_WAS_ABORTED))), TaskParameter).Compile();

        public static void PreventSynchronousContinuations(this Task t)
        {
            EnsureContinuationsWontBeCalledSynchronously(t);
        }
    }
}
