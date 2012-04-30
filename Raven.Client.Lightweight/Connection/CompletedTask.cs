#if !NET35
using System.Threading.Tasks;

namespace Raven.Client.Connection
{
	public class CompletedTask
	{
		public static implicit operator Task(CompletedTask _)
		{
			var tcs = new TaskCompletionSource<object>();
			tcs.SetResult(null);
			return tcs.Task;
		}

		public static CompletedTask<T> With<T>(T result)
		{
			return new CompletedTask<T>(result);
		}
	}

	public class CompletedTask<T>
	{
		public readonly T Result;

		public CompletedTask() : this(default(T)) { }

		public CompletedTask(T result)
		{
			this.Result = result;
		}

		public static implicit operator Task<T>(CompletedTask<T> t)
		{
			var tcs = new TaskCompletionSource<T>();
			tcs.SetResult(t.Result);
			return tcs.Task;
		}
	}
}
#endif