using System;
using System.IO;
using Raven.Storage.Managed.Data;

namespace Raven.Storage.Managed
{
	public class StorageMutator
	{
		private readonly Stream writer;
		private readonly Stream reader;
		private readonly TransactionalStorage.StorageTransaction transaction;

		public event Action OnCommit;

		private Tree indexes;
		private Tree identity;
		private Tree documents;
		private Tree documentsById;
		private Tree documentsByEtag;
		private Tree attachments;
		private Tree transactions;
		private Tree documentsInTx;
		private Tree tasksByIndex;
		private TreeOfBags mappedResultsByReduceKey;
		private TreeOfBags mappedResultsByDocumentId;
		private Queue tasks;
		private TreeOfQueues queues;

		private long tasksCount;
		private long documentsCount;
		private long attachmentsCount;

		public TreeOfBags MappedResultsByDocumentId
		{
			get
			{
				if (mappedResultsByDocumentId == null)
				{
					reader.Position = transaction.MappedResultsByDocumentIdPosition;
					mappedResultsByDocumentId = new TreeOfBags(reader, writer, StartMode.Open);
				}
				return mappedResultsByDocumentId;
			}
		}

		public TreeOfBags MappedResultsByReduceKey
		{
			get
			{
				if (mappedResultsByReduceKey == null)
				{
					reader.Position = transaction.MappedResultsByReduceKeyPosition;
					mappedResultsByReduceKey = new TreeOfBags(reader, writer, StartMode.Open);
				}
				return mappedResultsByReduceKey;
			}
		}

		public TreeOfQueues Queues
		{
			get
			{
				if (queues == null)
				{
					reader.Position = transaction.QueuesPosition;
					queues = new TreeOfQueues(reader, writer, StartMode.Open);
				}
				return queues;
			}
		}

		public Tree Indexes
		{
			get
			{
				if (indexes == null)
				{
					reader.Position = transaction.IndexesPosition;
					indexes = new Tree(reader, writer, StartMode.Open);
				}
				return indexes;
			}
		}

		public Tree Identity
		{
			get
			{
				if (identity == null)
				{
					reader.Position = transaction.IdentityPosition;
					identity = new Tree(reader, writer, StartMode.Open);
				}
				return identity;
			}
		}

		public Tree DocumentsInTransaction
		{
			get
			{
				if (documentsInTx == null)
				{
					reader.Position = transaction.DocumentsInTransactionPosition;
					documentsInTx = new Tree(reader, writer, StartMode.Open);
				}
				return documentsInTx;
			}
		}

		public Tree Transactions
		{
			get
			{
				if (transactions == null)
				{
					reader.Position = transaction.TransactionPosition;
					transactions = new Tree(reader, writer, StartMode.Open);
				}
				return transactions;
			}
		}

		public Tree DocumentsByEtag
		{
			get
			{
				if (documentsByEtag == null)
				{
					reader.Position = transaction.DocumentsByEtagPosition;
					documentsByEtag = new Tree(reader, writer, StartMode.Open);
				}
				return documentsByEtag;
			}
		}

		public Queue Tasks
		{
			get
			{
				if (tasks == null)
				{
					reader.Position = transaction.TasksPosition;
					tasks = new Queue(reader, writer, StartMode.Open);
				}
				return tasks;
			}
		}

		public Tree TasksByIndex
		{
			get
			{
				if (tasksByIndex == null)
				{
					reader.Position = transaction.TasksByIndexPosition;
					tasksByIndex = new Tree(reader, writer, StartMode.Open);
				}
				return tasksByIndex;
			}
		}

		public Tree Documents
		{
			get
			{
				if (documents == null)
				{
					reader.Position = transaction.DocumentsPosition;
					documents = new Tree(reader, writer, StartMode.Open);
				}
				return documents;
			}
		}

		public Tree Attachments
		{
			get
			{
				if (attachments == null)
				{
					reader.Position = transaction.AttachmentPosition;
					attachments = new Tree(reader, writer, StartMode.Open);
				}
				return attachments;
			}
		}

		public Tree DocumentsById
		{
			get
			{
				if (documentsById == null)
				{
					reader.Position = transaction.DocumentsByIdPosition;
					documentsById = new Tree(reader, writer, StartMode.Open);
				}
				return documentsById;
			}
		}

		internal StorageMutator(Stream writer, Stream reader, TransactionalStorage.StorageTransaction transaction)
		{
			this.writer = writer;
			this.reader = reader;
			this.transaction = transaction;
			attachmentsCount = transaction.AttachmentsCount;
			documentsCount = transaction.DocumentsCount;
			tasksCount = transaction.TasksCount;
		}

		public void IncrementDocumentCount()
		{
			documentsCount++;
		}

		public void DecrementDocumentCount()
		{
			documentsCount--;
		}

		public void IncrementAttachmentCount()
		{
			attachmentsCount++;
		}

		public void DecrementAttachmentCount()
		{
			attachmentsCount--;
		}


		public void IncrementTaskCount()
		{
			tasksCount++;
		}

		public void DecrementTaskCount()
		{
			tasksCount--;
		}

		internal void RaiseOnCommit()
		{
			var copy = OnCommit;
			if (copy == null)
				return;
			copy();
		}

		internal void Flush()
		{
			if (documents != null)
				documents.Flush();
			if (documentsById != null)
				documentsById.Flush();
			if (documentsByEtag != null)
				documentsByEtag.Flush();
			if (documentsInTx != null)
				documentsInTx.Flush();
			if (attachments != null)
				attachments.Flush();
			if (tasks != null)
				tasks.Flush();
			if (transactions != null)
				transactions.Flush();
			if (identity != null)
				identity.Flush();
			if (indexes != null)
				indexes.Flush();
			if (queues != null)
				queues.Flush();
			if (tasksByIndex != null)
				tasksByIndex.Flush();
			if(mappedResultsByDocumentId != null)
				mappedResultsByDocumentId.Flush();
			if (mappedResultsByReduceKey != null)
				mappedResultsByReduceKey.Flush();
		}

		internal TransactionalStorage.StorageTransaction CreateTransaction()
		{
			return new TransactionalStorage.StorageTransaction
			{
				DocumentsPosition = documents == null ? transaction.DocumentsPosition : documents.RootPosition,
				DocumentsCount = documentsCount,
				DocumentsByIdPosition = documentsById == null ? transaction.DocumentsByIdPosition : documentsById.RootPosition,
				DocumentsByEtagPosition =
					documentsByEtag == null ? transaction.DocumentsByEtagPosition : documentsByEtag.RootPosition,
				DocumentsInTransactionPosition =
					documentsInTx == null ? transaction.DocumentsInTransactionPosition : documentsInTx.RootPosition,
				AttachmentPosition = attachments == null ? transaction.AttachmentPosition : attachments.RootPosition,
				AttachmentsCount = attachmentsCount,
				TasksPosition = tasks == null ? transaction.TasksPosition : tasks.RootPosition,
				TasksCount = tasksCount,
				TransactionPosition = transactions == null ? transaction.TransactionPosition : transactions.RootPosition,
				IdentityPosition = identity == null ? transaction.IdentityPosition : identity.RootPosition,
				IndexesPosition = indexes == null ? transaction.IndexesPosition : indexes.RootPosition,
				QueuesPosition = queues == null ? transaction.QueuesPosition : queues.RootPosition,
				TasksByIndexPosition = tasksByIndex == null ? transaction.TasksByIndexPosition : tasksByIndex.RootPosition,
				MappedResultsByDocumentIdPosition = mappedResultsByDocumentId == null ? transaction.MappedResultsByDocumentIdPosition : mappedResultsByDocumentId.RootPosition,
				MappedResultsByReduceKeyPosition = mappedResultsByReduceKey == null ? transaction.MappedResultsByReduceKeyPosition : mappedResultsByReduceKey.RootPosition
			};
		}
	}
}