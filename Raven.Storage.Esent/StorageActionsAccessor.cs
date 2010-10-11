using System;
using Raven.Database.Storage;
using Raven.Database.Storage.StorageActions;
using Raven.Storage.Esent.StorageActions;

namespace Raven.Storage.Esent
{
	[CLSCompliant(false)]
	public class StorageActionsAccessor : IStorageActionsAccessor
	{
		private readonly DocumentStorageActions inner;

		[CLSCompliant(false)]
		public StorageActionsAccessor(DocumentStorageActions inner)
		{
			this.inner = inner;
		}

		public ITransactionStorageActions Transactions
		{
			get { return inner; }
		}

		public IDocumentStorageActions Documents
		{
			get { return inner; }
		}

		public IQueueStorageActions Queue
		{
			get { return inner; }
		}

		public ITasksStorageActions Tasks
		{
			get { return inner; }
		}

		public IAttachmentsStorageActions Attachments
		{
			get { return inner; }
		}

		public IIndexingStorageActions Indexing
		{
			get { return inner; }
		}

		public IGeneralStorageActions General
		{
			get { return inner; }
		}

		public IMappedResultsStorageAction MappedResults
		{
			get { return inner; }
		}

		public event Action OnCommit
		{
			add { inner.OnCommit += value; }
			remove { inner.OnCommit -= value; }
		}
	}
}
