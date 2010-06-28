using System;
using System.IO;
using Raven.Database.Storage;
using Raven.Database.Storage.StorageActions;
using Raven.Storage.Managed.StroageActions;

namespace Raven.Storage.Managed
{
	public class StorageActionsAccessor : IStorageActionsAccessor
	{
		private AttachmentsStorageActions attachmentsStorageActions;
		private DocumentStorageActions documents;
		private IndexingStorageActions indexes;
		private readonly StorageMutator mutator;
		private readonly StorageReader viewer;
		private readonly Stream reader;
		private readonly Stream writer;
		private GeneralStorageActions generalStorageActions;
		private QueueStorageActions queues;
		private TasksStorageActions tasks;
		private MappedResultsStorageAction mappedResults;

		public StorageActionsAccessor(StorageMutator mutator, StorageReader viewer, Stream reader, Stream writer)
		{
			this.mutator = mutator;
			this.viewer = viewer;
			this.reader = reader;
			this.writer = writer;
		}

		public IMappedResultsStorageAction MappedResults
		{
			get
			{
				if(mappedResults == null)
				{
					mappedResults = new MappedResultsStorageAction();
					Init(mappedResults);
				}
				return mappedResults;
			}
		}

		public event Action OnCommit;

		public void RaiseCommitEvent()
		{
			var copy = OnCommit;
			if (copy != null)
				copy();
		}

		public IIndexingStorageActions Indexing
		{
			get
			{
				if (indexes == null)
				{
					indexes = new IndexingStorageActions();
					Init(indexes);
				}
				return indexes;
			}
		}


		public IAttachmentsStorageActions Attachments
		{
			get
			{
				if (attachmentsStorageActions==null)
				{
					attachmentsStorageActions = new AttachmentsStorageActions();
					Init(attachmentsStorageActions);
				}
				return attachmentsStorageActions;
			}
		}

		public IDocumentStorageActions Documents
		{
			get
			{
				if (documents == null)
				{
					documents = new DocumentStorageActions();
					Init(documents);
				}
				return documents;
			}
		}

		public IGeneralStorageActions General
		{
			get
			{
				if (generalStorageActions == null)
				{
					generalStorageActions = new GeneralStorageActions();
					Init(generalStorageActions);
				}
				return generalStorageActions;
			}
		}

		public ITransactionStorageActions Transactions
		{
			get
			{
				if (documents == null)
				{
					documents = new DocumentStorageActions();
					Init(documents);
				}
				return documents;
			}
		}

		public IQueueStorageActions Queue
		{
			get
			{
				if (queues == null)
				{
					queues = new QueueStorageActions();
					Init(queues);
				}
				return queues;
			}
		}

		public ITasksStorageActions Tasks
		{
			get
			{
				if (tasks == null)
				{
					tasks = new TasksStorageActions();
					Init(tasks);
				}
				return tasks;
			}
		}


		private void Init(AbstractStorageActions actions)
		{
			actions.Mutator = mutator;
			actions.Viewer = viewer;
			actions.Reader = reader;
			actions.Writer = writer;
		}
	}
}