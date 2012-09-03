using System;
using System.Data;
using Raven.Database.Impl;
using Raven.Database.Tasks;
using Raven.Storage.Managed;

namespace Raven.Database.Storage.RAM
{
	public class RamStorageActionsAccessor : IStorageActionsAccessor
	{
		private readonly RamState state;
		private readonly IUuidGenerator generator;

		public RamStorageActionsAccessor(RamState state, IUuidGenerator generator)
		{
			this.state = state;
			this.generator = generator;
			Lists = new RamListsStorageActions(state, generator);
			Queue = new RamQueueStorageActions(state, generator);
			Tasks = new RamTasksStorageActions(state, generator);
			General = new RamGeneralStorageActions(state);
			Attachments = new RamAttachmentsStorageActions(state, generator);
			Documents = new RamDocumentsStorageActions(state, generator);
			Transactions = new RamTransactionStorageActions(state, generator);

		}

		public void Dispose()
		{
			
		}

		public IQueueStorageActions Queue { get; private set; }
		public IListsStorageActions Lists { get; private set; }
		public ITasksStorageActions Tasks { get; private set; }
		public IGeneralStorageActions General { get; private set; }
		public IAttachmentsStorageActions Attachments { get; private set; }
		public IDocumentStorageActions Documents { get; private set; }

		public ITransactionStorageActions Transactions { get; private set; }
		
		public IStalenessStorageActions Staleness { get; private set; }
		public IIndexingStorageActions Indexing { get; private set; }
		public IMappedResultsStorageAction MapReduce { get; private set; }
		
		public event Action OnCommit;

		public bool IsWriteConflict(Exception exception)
		{
			return exception is DBConcurrencyException;
		}

		public T GetTask<T>(Func<T, bool> predicate, T newTask) where T : Task
		{
			throw new NotImplementedException();
		}
	}
}