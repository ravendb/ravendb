using Raven.Database.Tasks;

namespace Raven.Database.Cache
{
	public class TaskCache : CacheBuildingBlock<Task>
	{
		public static void RememberTask(int taskId, Task task)
		{
			Remember("tasks/" + taskId, task);
		}

		public static Task ParseTask(int taskId, string taskAsString)
		{
			return Parse("tasks/" + taskId, () => Task.ToTask(taskAsString))
				.Clone();
		}

	}
}