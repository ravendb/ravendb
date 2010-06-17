using System;
using Raven.Database.Tasks;

namespace Raven.Database.Storage.StorageActions
{
	public interface ITasksStorageActions
	{
		bool DoesTasksExistsForIndex(string name, DateTime? cutOff);
		void AddTask(Task task);
		bool HasTasks { get; }
		int ApproximateTaskCount { get; }
		Task GetMergedTask(out int countOfMergedTasks);
		void MergeSimilarTasks(Task task, out int taskCount);
	}
}