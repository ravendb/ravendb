using System.Linq;
using Raven.Client.Extensions;

namespace System.Threading.Tasks
{
	public static class TaskEx
	{
		public static Task Delay(int milliseconds)
		{
			return Time.Delay(TimeSpan.FromMilliseconds(milliseconds));
		}
		public static Task Delay(TimeSpan value)
		{
			return Time.Delay(value);
		}

		public static Task<Task> WhenAny(Task task, Task delay)
		{
			if (task == null)
				throw new ArgumentNullException("task");
			var tcs = new TaskCompletionSource<Task>();
			var tasks = new Task[1];
			tasks[0] = task;
			Task.Factory.ContinueWhenAny(tasks, completed => tcs.TrySetResult(completed), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
			return tcs.Task;
		}
	}
}
