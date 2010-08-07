using System;
using Raven.Database.Tasks;

namespace Raven.Database.Storage.StorageActions
{
	public interface ITasksStorageActions
	{
		bool IsIndexStale(string name, DateTime? cutOff);
		void AddTask(Task task);
		bool HasTasks { get; }
		long ApproximateTaskCount { get; }
		Task GetMergedTask(out int countOfMergedTasks);
	}
}