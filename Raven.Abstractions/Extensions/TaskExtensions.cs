using System;
using System.Net;
using System.Security;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;

namespace Raven.Abstractions.Extensions
{
	public static class TaskExtensions
	{
		public static void AssertNotFailed(this Task task)
		{
			if (task.IsFaulted)
				task.Wait(); // would throw
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

	    public static async Task<bool> WaitWithTimeout(this Task task, TimeSpan? timeout)
		{
			if (timeout == null)
			{
				await task;
				return true;
			}
			if (task == await Task.WhenAny(task, Task.Delay(timeout.Value)))
				return true;
			return false;
		}
	}
}