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
			Transactions = new TransactionStorageActions(storage, generator, documentCodecs);
			Documents = new DocumentsStorageActions(storage, Transactions, generator, documentCodecs, documentCacher);
			Indexing = new IndexingStorageActions(storage);
			MapReduce = new MappedResultsStorageAction(storage, generator, documentCodecs);
			Queue = new QueueStorageActions(storage, generator);
			Tasks = new TasksStorageActions(storage, generator);
			Staleness = new StalenessStorageActions(storage);
			Lists = new ListsStorageActions(storage, generator);
		}

		public IListsStorageActions Lists { get; private set; }

		public ITransactionStorageActions Transactions { get; private set; }

		public IDocumentStorageActions Documents { get; private set; }

		public IQueueStorageActions Queue { get; private set; }

		public ITasksStorageActions Tasks { get; private set; }

		public IStalenessStorageActions Staleness { get; private set; }

		public IAttachmentsStorageActions Attachments { get; private set; }

		public IIndexingStorageActions Indexing { get; private set; }

		public IGeneralStorageActions General { get; private set; }

		public IMappedResultsStorageAction MapReduce { get; private set; }

		public event Action OnCommit;

		public bool IsWriteConflict(Exception exception)
		{
			return exception is ConcurrencyException;
		}

		private readonly List<Task> tasks = new List<Task>();

		public T GetTask<T>(Func<T, bool> predicate, T newTask) where T : Task
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
		public void AfterCommit(JsonDocument doc, Action<JsonDocument[]> afterCommit)
		{
			afterCommitAction = afterCommit;
			if (docsForCommit == null)
			{
				docsForCommit = new List<JsonDocument>();
				OnCommit += () => afterCommitAction(docsForCommit.ToArray());
			}
			docsForCommit.Add(doc);
		}

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
			var handler = OnCommit;
			if (handler != null)
				handler();
		}

		public void Dispose()
		{
			Indexing.Dispose();
		}
	}
}
