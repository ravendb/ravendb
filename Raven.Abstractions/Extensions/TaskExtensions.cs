using System;
using System.Net;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
#if DNXCORE50
using Raven.Abstractions.Exceptions;
#endif

namespace Raven.Abstractions.Extensions
{
    public static class TaskExtensions
    {
        public static Task AssertNotFailed(this Task task)
        {
            if (task.IsFaulted)
                task.Wait(); // would throw

            return task;
        }

        //credit for idea : https://blogs.msdn.microsoft.com/pfxteam/2009/06/01/tasks-and-unhandled-exceptions/
        public static Task IgnoreUnobservedExceptions(this Task task)
        {
            return task.ContinueWith(t =>
                {
                    GC.KeepAlive(t.Exception);
                },
                TaskContinuationOptions.ExecuteSynchronously);
        }

        public static Task<T> ConvertSecurityExceptionToServerNotFound<T>(this Task<T> parent)
        {
            return parent.ContinueWith(task =>
            {
                if (task.IsFaulted)
                {
                    var exception = task.Exception.ExtractSingleInnerException();
                    if (exception is SecurityException)
                        throw new WebException("Could not contact server.\r\nGot security error because RavenDB wasn't able to contact the database to get ClientAccessPolicy.xml permission.", exception);
                }
                return task;
            }).Unwrap();
        }

        public static Task<T> AddUrlIfFaulting<T>(this Task<T> parent, Uri uri)
        {
            return parent.ContinueWith(
                task =>
                {
                    if (task.IsFaulted)
                    {
                        var e = task.Exception.ExtractSingleInnerException();
                        if (e != null) 
                            e.Data["Url"] = uri;
                    }

                    return task;
                }).Unwrap();
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
            if (token == default (CancellationToken))
                return task;

            return task.ContinueWith(t => t.ConfigureAwait(false).GetAwaiter().GetResult(), token);
        }

        public static void ThrowCancellationIfNotDefault(this CancellationToken token)
        {
            if(token != default (CancellationToken))
                token.ThrowIfCancellationRequested();
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
    }
}
