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

		public static async Task<bool> WaitWithTimeout(this Task task, TimeSpan? timeout)
		{
			if (timeout == null)
			{
				await task;
				return true;
			}
#if NET45
			if (task == await Task.WhenAny(task, Task.Delay(timeout.Value)))
#else
			if (task == await TaskEx.WhenAny(task, TaskEx.Delay(timeout.Value)))
#endif
				return true;
			return false;
		}
	}
}