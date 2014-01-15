using System;
using System.Threading.Tasks;

namespace Raven.Client.RavenFS.Util
{
	public static class TaskExtensions
	{
		public static Task<T> FromException<T>(Exception ex)
		{
			var taskCompletionSource = new TaskCompletionSource<T>();
			taskCompletionSource.SetException(ex);
			return taskCompletionSource.Task;
		}

		public static Task FromException(Exception ex)
		{
			return FromException<bool>(ex);
		}

		public static void AssertNotFailed(this Task task)
		{
			if (task.IsFaulted)
				task.Wait(); // would throw
		}
	}
}
