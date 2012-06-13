#if !NET35
using System.Threading.Tasks;

namespace Raven.Client.Connection
{
	public class CompletedTask : CompletedTask<object>
	{
		public new Task Task
		{
			get { return base.Task; }
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

		public Task<T> Task
		{
			get
			{
				var tcs = new TaskCompletionSource<T>();
				tcs.SetResult(Result);
				return tcs.Task;
			}
		}

		public static implicit operator Task<T>(CompletedTask<T> t)
		{
			return t.Task;
		}

		public static implicit operator Task(CompletedTask<T> t)
		{
			return t.Task;
		}
	}
}
#endif