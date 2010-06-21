using System;
using Raven.Database.Storage.StorageActions;

namespace Raven.Database.Storage
{
	public interface ITransactionalStorage
	{
		Guid Id { get; }
		void Batch(Action<IStorageActionsAccessor> action);
		void ExecuteImmediatelyOrRegisterForSyncronization(Action action);
		bool Initialize();
		void Dispose();
	}

	public interface IStorageActionsAccessor
	{
		ITransactionStorageActions Transactions { get; }
		IDocumentStorageActions Documents { get; }
		IQueueStorageActions Queue { get; }
		ITasksStorageActions Tasks { get; }
		IAttachmentsStorageActions Attachments { get; }
		IIndexingStorageActions Indexing { get; }
		IGeneralStorageActions General { get; }
		IMappedResultsStorageAction MappedResults { get; }
		event Action OnCommit;
	}

}