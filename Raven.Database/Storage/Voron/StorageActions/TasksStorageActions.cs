// -----------------------------------------------------------------------
//  <copyright file="TasksStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron.StorageActions
{
	using System;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Logging;
	using Raven.Database.Impl;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Database.Tasks;
	using Raven.Json.Linq;

	using global::Voron;
	using global::Voron.Impl;

	public class TasksStorageActions : StorageActionsBase, ITasksStorageActions
	{
		private static readonly ILog Logger = LogManager.GetCurrentClassLogger();

		private readonly TableStorage tableStorage;

		private readonly IUuidGenerator generator;

		private readonly WriteBatch writeBatch;

		public TasksStorageActions(TableStorage tableStorage, IUuidGenerator generator, SnapshotReader snapshot, WriteBatch writeBatch)
			: base(snapshot)
		{
			this.tableStorage = tableStorage;
			this.generator = generator;
			this.writeBatch = writeBatch;
		}

		public void AddTask(Task task, DateTime addedAt)
		{
			var tasksByType = this.tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByType);
			var tasksByIndex = this.tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndex);
			var tasksByIndexAndType = this.tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndexAndType);

			var type = task.GetType().FullName;
			var index = task.Index ?? string.Empty;
			var id = this.generator.CreateSequentialUuid(UuidType.Tasks);
			var idAsString = id.ToString();

			this.tableStorage.Tasks.Add(
				this.writeBatch,
				idAsString,
				new RavenJObject
				{
					{ "index", task.Index },
					{ "id", id.ToByteArray() },
					{ "time", addedAt },
					{ "type", type },
					{ "task", task.AsBytes() }
				}, 0);

			tasksByType.MultiAdd(this.writeBatch, type, idAsString);
			tasksByIndex.MultiAdd(this.writeBatch, index, idAsString);
			tasksByIndexAndType.MultiAdd(this.writeBatch, this.CreateKey(index, type), idAsString);
		}

		public bool HasTasks
		{
			get { return this.ApproximateTaskCount > 0; }
		}

		public long ApproximateTaskCount
		{
			get
			{
				return this.tableStorage.GetEntriesCount(this.tableStorage.Tasks);
			}
		}

		public T GetMergedTask<T>() where T : Task
		{
			var type = typeof(T).FullName;
			var tasksByType = this.tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByType);

			using (var iterator = tasksByType.MultiRead(this.Snapshot, type))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return null;

				do
				{
					ushort version;
					var value = this.LoadJson(this.tableStorage.Tasks, iterator.CurrentKey, out version);

					Task task;
					try
					{
						task = Task.ToTask(value.Value<string>("type"), value.Value<byte[]>("task"));
					}
					catch (Exception e)
					{
						Logger.ErrorException(
							string.Format("Could not create instance of a task: {0}", value),
							e);
						continue;
					}

					this.MergeSimilarTasks(task, value.Value<byte[]>("id"));
					this.RemoveTask(iterator.CurrentKey, task.Index, type);

					return (T)task;
				}
				while (iterator.MoveNext());
			}

			return null;
		}

		private void MergeSimilarTasks(Task task, byte[] taskId)
		{
			var id = Etag.Parse(taskId);
			var type = task.GetType().FullName;
			var tasksByIndexAndType = this.tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndexAndType);

			using (var iterator = tasksByIndexAndType.MultiRead(this.Snapshot, this.CreateKey(task.Index, type)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				int totalTaskCount = 0;

				do
				{
					var currentId = Etag.Parse(iterator.CurrentKey.ToString());
					if (currentId == id)
						continue;

					ushort version;
					var value = this.LoadJson(this.tableStorage.Tasks, iterator.CurrentKey, out version);

					Task existingTask;
					try
					{
						existingTask = Task.ToTask(value.Value<string>("type"), value.Value<byte[]>("task"));
					}
					catch (Exception e)
					{
						Logger.ErrorException(
							string.Format("Could not create instance of a task: {0}", value),
							e);

						this.RemoveTask(iterator.CurrentKey, task.Index, type);
						continue;
					}

					task.Merge(existingTask);
					this.RemoveTask(iterator.CurrentKey, task.Index, type);

					if (totalTaskCount++ > 1024)
						break;
				}
				while (iterator.MoveNext());
			}
		}

		private void RemoveTask(Slice taskId, string index, string type)
		{
			index = index ?? string.Empty;

			var tasksByType = this.tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByType);
			var tasksByIndex = this.tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndex);
			var tasksByIndexAndType = this.tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndexAndType);

			this.tableStorage.Tasks.Delete(this.writeBatch, taskId);
			tasksByType.MultiDelete(this.writeBatch, type, taskId);
			tasksByIndex.MultiDelete(this.writeBatch, index, taskId);
			tasksByIndexAndType.MultiDelete(this.writeBatch, this.CreateKey(index, type), taskId);
		}
	}
}