#if !NET_3_5
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
	}
}
#endif