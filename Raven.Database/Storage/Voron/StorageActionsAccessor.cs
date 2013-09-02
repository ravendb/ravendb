using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.MEF;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Tasks;
using Voron.Impl;

namespace Raven.Database.Storage.Voron
{
	using Raven.Database.Storage.Voron.Impl;

	public class StorageActionsAccessor : IStorageActionsAccessor
	{
		private readonly WriteBatch writeBatch;
		private readonly SnapshotReader snapshot;

		public StorageActionsAccessor(IUuidGenerator generator,
									  OrderedPartCollection<AbstractDocumentCodec> documentCodecs,
									  IDocumentCacher documentCacher,
									  WriteBatch writeBatch,
									  SnapshotReader snapshot,
									  TableStorage storage)
		{
			this.writeBatch = writeBatch;
			this.snapshot = snapshot;
			Documents = new DocumentsStorageActions(generator, documentCodecs, documentCacher, writeBatch, snapshot, storage.Documents);
			Indexing = new IndexingStorageActions(storage, snapshot, writeBatch);
			Queue = new QueueStorageActions(storage, generator, snapshot, writeBatch);
		}


		public void Dispose()
		{
			//do nothing for now
		}

		public IDocumentStorageActions Documents { get; private set; }

		public IQueueStorageActions Queue { get; private set; }

		public IListsStorageActions Lists { get; private set; }

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

		public T GetTask<T>(Func<T, bool> predicate, T newTask) where T : Task
		{
			throw new NotImplementedException();
		}

		private Action<JsonDocument[]> afterCommitAction;
		private List<JsonDocument> docsForCommit;

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

		public void SaveAllTasks()
		{
			//method stub
		}
	}
}
