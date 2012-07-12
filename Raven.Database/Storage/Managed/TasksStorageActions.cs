//-----------------------------------------------------------------------
// <copyright file="TasksStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using NLog;
using Raven.Database.Impl;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Json.Linq;
using Raven.Storage.Managed.Impl;
using System.Linq;

namespace Raven.Storage.Managed
{
	public class TasksStorageActions : ITasksStorageActions
	{
		private readonly TableStorage storage;
		private readonly IUuidGenerator generator;
		private static readonly Logger logger = LogManager.GetCurrentClassLogger();

		public TasksStorageActions(TableStorage storage, IUuidGenerator generator)
		{
			this.storage = storage;
			this.generator = generator;
		}

		public void AddTask(Task task, DateTime addedAt)
		{
			storage.Tasks.Put(new RavenJObject
			{
				{"index", task.Index},
				{"id", generator.CreateSequentialUuid().ToByteArray()},
				{"time", addedAt},
				{"type", task.GetType().FullName},
				{"mergable", task.SupportsMerging}
			}, task.AsBytes());
		}

		public bool HasTasks
		{
			get { return ApproximateTaskCount > 0; }
		}

		public long ApproximateTaskCount
		{
			get { return storage.Tasks.Count; }
		}

		public T GetMergedTask<T>() where T : Task
		{
			foreach (var readResult in storage.Tasks)
			{
				var taskType = readResult.Key.Value<string>("type");
				if(taskType != typeof(T).FullName)
					continue;

				Task task;
				try
				{
					task = Task.ToTask(taskType, readResult.Data());
				}
				catch (Exception e)
				{
					logger.ErrorException(
						string.Format("Could not create instance of a task: {0}", readResult.Key),
						e);
					continue;
				}
				MergeSimilarTasks(task, readResult.Key.Value<byte[]>("id"));
				storage.Tasks.Remove(readResult.Key);
				return (T)task;
			}
			return null;
		}

		private void MergeSimilarTasks(Task task, byte [] taskId)
		{
			if (task.SupportsMerging == false)
				return;

			var taskType = task.GetType().FullName;
			var keyForTaskToTryMergings = storage.Tasks["ByIndexAndType"].SkipTo(new RavenJObject
			{
				{"index", task.Index},
				{"type", taskType},
			})
			.Where(x => new Guid(x.Value<byte[]>("id")) != new Guid(taskId))
				.TakeWhile(x =>
						   StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("index"), task.Index) &&
						   StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("type"), taskType)
				);

			foreach (var keyForTaskToTryMerging in keyForTaskToTryMergings)
			{
				var readResult = storage.Tasks.Read(keyForTaskToTryMerging);
				if(readResult == null)
					continue;
				Task existingTask;
				try
				{
					existingTask = Task.ToTask(readResult.Key.Value<string>("type"), readResult.Data());
				}
				catch (Exception e)
				{
					logger.ErrorException(
						string.Format("Could not create instance of a task: {0}", readResult.Key),
						e);
					storage.Tasks.Remove(keyForTaskToTryMerging);
					continue;
				}

				if (task.TryMerge(existingTask) == false)
					continue;

				storage.Tasks.Remove(keyForTaskToTryMerging);

				if (task.SupportsMerging == false)
					return;
			}
		}

	}
}