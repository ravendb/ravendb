using System;
using Raven.Database.Tasks;

namespace Raven.Database.Storage.StorageActions
{
	public interface ITasksStorageActions
	{
		void AddTask(Task task, DateTime addedAt);
		bool HasTasks { get; }
		long ApproximateTaskCount { get; }
		Task GetMergedTask(out int countOfMergedTasks);
	}
}
