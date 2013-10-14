//-----------------------------------------------------------------------
// <copyright file="StorageActionsAccessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.MEF;
using Raven.Database;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Storage.Managed.Impl;
using System.Linq;

namespace Raven.Storage.Managed
{
	public class StorageActionsAccessor : IStorageActionsAccessor
	{
		private readonly DateTime createdAt = SystemTime.UtcNow;

		public StorageActionsAccessor(TableStorage storage, IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs, IDocumentCacher documentCacher)
		{
			General = new GeneralStorageActions(storage);
			Attachments = new AttachmentsStorageActions(storage, generator);
			Documents = new DocumentsStorageActions(storage, generator, documentCodecs, documentCacher);
			Indexing = new IndexingStorageActions(storage);
			mappedResultsStorageAction = new MappedResultsStorageAction(storage, generator, documentCodecs);
			MapReduce = mappedResultsStorageAction;
			Queue = new QueueStorageActions(storage, generator);
			Tasks = new TasksStorageActions(storage, generator);
			Staleness = new StalenessStorageActions(storage);
			Lists = new ListsStorageActions(storage, generator);
		}

		public IListsStorageActions Lists { get; private set; }


		public IDocumentStorageActions Documents { get; private set; }

		public IQueueStorageActions Queue { get; private set; }

		public ITasksStorageActions Tasks { get; private set; }

		public IStalenessStorageActions Staleness { get; private set; }

		public IAttachmentsStorageActions Attachments { get; private set; }

		public IIndexingStorageActions Indexing { get; private set; }

		public IGeneralStorageActions General { get; private set; }

		public IMappedResultsStorageAction MapReduce { get; private set; }

	    public bool IsNested { get; set; }

	    public event Action OnStorageCommit;

		public bool IsWriteConflict(Exception exception)
		{
			return exception is ConcurrencyException;
		}

		private readonly List<DatabaseTask> tasks = new List<DatabaseTask>();

		public T GetTask<T>(Func<T, bool> predicate, T newTask) where T : DatabaseTask
		{
			T task = tasks.OfType<T>().FirstOrDefault(predicate);
			if (task == null)
			{
				tasks.Add(newTask);
				return newTask;
			}
			return task;
		}


		private Action<JsonDocument[]> afterCommitAction;
		private List<JsonDocument> docsForCommit;
		private readonly MappedResultsStorageAction mappedResultsStorageAction;

		public void AfterStorageCommitBeforeWorkNotifications(JsonDocument doc, Action<JsonDocument[]> afterCommit)
		{
			afterCommitAction = afterCommit;
			if (docsForCommit == null)
			{
				docsForCommit = new List<JsonDocument>();
				OnStorageCommit += () => afterCommitAction(docsForCommit.ToArray());
			}
			docsForCommit.Add(doc);
		}

		[DebuggerNonUserCode]
		public void SaveAllTasks()
		{
			foreach (var task in tasks)
			{
				Tasks.AddTask(task, createdAt);
			}
		}

		[DebuggerNonUserCode]
		public void InvokeOnCommit()
		{
			var handler = OnStorageCommit;
			if (handler != null)
				handler();
		}

		public void Dispose()
		{
			Indexing.Dispose();
		}
	}
}
