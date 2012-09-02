using System;
using System.Linq;
using Raven.Database.Impl;
using Raven.Database.Tasks;

namespace Raven.Database.Storage.RAM
{
	public class RamTasksStorageActions : ITasksStorageActions
	{
		private readonly IUuidGenerator generator;
		private readonly RamState state;

		public RamTasksStorageActions(RamState state, IUuidGenerator generator)
		{
			this.state = state;
			this.generator = generator;
		}

		#region ITasksStorageActions Members

		public void AddTask(Task task, DateTime addedAt)
		{
			state.Tasks.GetOrAdd(task.GetType().FullName)
				.Set(generator.CreateSequentialUuid(), task.AsBytes());
		}

		public T GetMergedTask<T>() where T : Task
		{
			Task result = null;
			foreach (var task in state.Tasks.GetOrAdd(typeof (T).FullName)
					.OrderBy(x => x.Key)
					.Select(rawTask => Task.ToTask(typeof (T).FullName, rawTask.Value)))
			{
				if (result == null)
					result = task;
				else
					result.Merge(task);
			}
			return (T) result;
		}

		public bool HasTasks
		{
			get { return state.Tasks.Any(x => x.Value.Count > 0); }
		}

		public long ApproximateTaskCount
		{
			get { return state.Tasks.Sum(x => x.Value.Count); }
		}

		#endregion
	}
}