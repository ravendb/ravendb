using System.IO;
using Raven.Storage.Managed.Data;

namespace Raven.Storage.Managed
{
	public class StorageReader
	{
		private readonly Stream reader;
		private readonly TransactionalStorage.StorageTransaction transaction;

		public long DocumentCount
		{
			get { return transaction.DocumentsCount; }
		}

		public long AttachmentCount
		{
			get { return transaction.AttachmentsCount; }
		}

		internal StorageReader(Stream reader, TransactionalStorage.StorageTransaction transaction)
		{
			this.reader = reader;
			this.transaction = transaction;
		}

		private Tree documents;
		private Tree attachments;
		private Tree tasksByIndex;
		private Tree documentsById;
		private Tree documentsByEtag;
		private Tree documentsInTx;
		private Tree transactions;
		private Tree indexes;
		private TreeOfBags mappedResultsByReduceKey;

		public long TaskCount
		{
			get
			{
				return transaction.TasksCount;
			}
		}

		public Tree Indexes
		{
			get
			{
				if (indexes == null)
				{
					reader.Position = transaction.IndexesPosition;
					indexes = new Tree(reader, Stream.Null, StartMode.Open);
				}
				return indexes;
			}
		}


		public Tree TasksByIndex
		{
			get
			{
				if (tasksByIndex == null)
				{
					reader.Position = transaction.TasksByIndexPosition;
					tasksByIndex = new Tree(reader, Stream.Null, StartMode.Open);
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
					documents = new Tree(reader, Stream.Null, StartMode.Open);
				}
				return documents;
			}
		}


		public Tree Transactions
		{
			get
			{
				if (transactions == null)
				{
					reader.Position = transaction.TransactionPosition;
					transactions = new Tree(reader, Stream.Null, StartMode.Open);
				}
				return transactions;
			}
		}

		public Tree DocumentsInTransaction
		{
			get
			{
				if (documentsInTx == null)
				{
					reader.Position = transaction.DocumentsInTransactionPosition;
					documentsInTx = new Tree(reader, Stream.Null, StartMode.Open);
				}
				return documentsInTx;
			}
		}

		public Tree DocumentsById
		{
			get
			{
				if (documentsById == null)
				{
					reader.Position = transaction.DocumentsByIdPosition;
					documentsById = new Tree(reader, Stream.Null, StartMode.Open);
				}
				return documentsById;
			}
		}

		public Tree DocumentsByEtag
		{
			get
			{
				if (documentsByEtag == null)
				{
					reader.Position = transaction.DocumentsByEtagPosition;
					documentsByEtag = new Tree(reader, Stream.Null, StartMode.Open);
				}
				return documentsByEtag;
			}
		}

		public Tree Attachments
		{
			get
			{
				if (attachments == null)
				{
					reader.Position = transaction.AttachmentPosition;
					attachments = new Tree(reader, Stream.Null, StartMode.Open);
				}
				return attachments;
			}
		}

		public TreeOfBags MappedResultsByReduceKey
		{
			get
			{
				if (mappedResultsByReduceKey == null)
				{
					reader.Position = transaction.MappedResultsByReduceKeyPosition;
					mappedResultsByReduceKey = new TreeOfBags(reader, Stream.Null, StartMode.Open);
				}
				return mappedResultsByReduceKey;
			}
		}
	}
}