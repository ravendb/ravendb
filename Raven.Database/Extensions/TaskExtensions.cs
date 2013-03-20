using System.Threading.Tasks;

namespace Raven.Database.Extensions
{
	public static class TaskExtensions
	{
		public static bool Completed(this Task task)
		{
			return task.IsCompleted || task.IsFaulted || task.IsCanceled;
		}
	}
}