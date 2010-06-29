using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Raven.Database;
using Raven.Database.Storage;
using Raven.Storage.Managed.Data;

namespace Raven.Storage.Managed
{
	public class TransactionalStorage : ITransactionalStorage
	{
		private readonly Action onCommit;
		public static readonly byte[] HeaderSignature = new Guid("FD9507A5-9B86-4FE5-977D-4E1E89ADCB1A").ToByteArray();
		public static readonly byte[] TransactionSignature = new Guid("4786E847-70E9-4D87-AE5B-C26EE5533D8A").ToByteArray();
		public static readonly Guid HeaderSignatureGuid = new Guid(HeaderSignature);
		public static readonly Guid TransactionSignatureGuid = new Guid(TransactionSignature);
		private readonly FileStream writer;
		private readonly string storageFile;
		private readonly object locker = new object();
		private readonly BinaryWriterWith7BitEncoding binaryWriter;
		private StorageTransaction transaction;
		private readonly bool isNewDatabase;
		private bool disposed;
		private readonly ThreadLocal<IStorageActionsAccessor> current = new ThreadLocal<IStorageActionsAccessor>();
		private const int Version = 1;

		public TransactionalStorage(RavenConfiguration configuration, Action onCommit)
			: this(configuration.DataDirectory)
		{
			this.onCommit = onCommit;
		}

		public TransactionalStorage(string path)
		{
			if (Directory.Exists(path) == false)
				Directory.CreateDirectory(path);

			storageFile = Path.Combine(path, "storage.raven");
			writer = new FileStream(storageFile,
									FileMode.OpenOrCreate,
									FileAccess.ReadWrite,
									FileShare.Delete | FileShare.Read,
									16 * 1024,
									FileOptions.WriteThrough | FileOptions.SequentialScan);
			try
			{
				binaryWriter = new BinaryWriterWith7BitEncoding(writer);
				isNewDatabase = writer.Length == 0;
				if (isNewDatabase)
					CreateFromScratch();
				TryReadingExistingFile();
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}
		}

		private void TryReadingExistingFile()
		{
			using (var reader = OpenReader())
			using (var binaryReader = new BinaryReaderWith7BitEncoding(reader))
			{
				if (new Guid(binaryReader.ReadBytes(16)) != HeaderSignatureGuid)
					throw new Exceptions.InvalidFileFormatException("File signature is invalid, probably not a valid Raven storage file, or a corrupted one");
				var version = binaryReader.Read7BitEncodedInt();
				if (version != Version)
					throw new Exceptions.InvalidFileFormatException("File signature is valid, but the version information is " +
						version + ", while " + Version + " was expected");
				Id = new Guid(binaryReader.ReadBytes(16));

				var pos = reader.Length;
				while (pos > 16)
				{
					reader.Position = pos - 16;
					if (new Guid(binaryReader.ReadBytes(16)) == TransactionSignatureGuid)
					{
						reader.Position = pos - 24; // move to the position of the transaction itself
						var txPos = binaryReader.ReadInt64();
						reader.Position = txPos;
						transaction = new JsonSerializer().Deserialize<StorageTransaction>(new BsonReader(reader));
						return;
					}
					pos -= 1;
				}
				throw new Exceptions.InvalidFileFormatException("Could not find a valid transaction in the file");
			}
		}


		private FileStream OpenReader()
		{
			return new FileStream(storageFile,
								  FileMode.Open,
								  FileAccess.Read,
								  FileShare.ReadWrite | FileShare.Delete,
								  16 * 1024,
								  FileOptions.RandomAccess);
		}

		private void CreateFromScratch()
		{
			binaryWriter.Write(HeaderSignature);
			binaryWriter.Write7BitEncodedInt(Version);
			binaryWriter.Write(Guid.NewGuid().ToByteArray());
			using (var reader = OpenReader())
			{
				var docs = new Tree(reader, writer, StartMode.Create);
				var docsInTx = new Tree(reader, writer, StartMode.Create);
				var docById = new Tree(reader, writer, StartMode.Create);
				var docByEtag = new Tree(reader, writer, StartMode.Create);
				var tx = new Tree(reader, writer, StartMode.Create);
				var attachments = new Tree(reader, writer, StartMode.Create);
				var tasks = new Queue(reader, writer, StartMode.Create);
				var tasksByIndex = new Tree(reader, writer, StartMode.Create);
				var identity = new Tree(reader, writer, StartMode.Create);
				var indexes = new Tree(reader, writer, StartMode.Create);
				var queues = new TreeOfQueues(reader, writer, StartMode.Create);
				var mapResultsByDocId = new TreeOfBags(reader, writer, StartMode.Create);
				var mapResultsByReduceKey = new TreeOfBags(reader, writer, StartMode.Create);
				docs.Flush();
				docsInTx.Flush();
				docById.Flush();
				docByEtag.Flush();
				tx.Flush();
				attachments.Flush();
				tasks.Flush();
				tasksByIndex.Flush();
				identity.Flush();
				indexes.Flush();
				queues.Flush();
				mapResultsByDocId.Flush();
				mapResultsByReduceKey.Flush();

				WriteTransaction(new StorageTransaction
				{
					AttachmentPosition = attachments.RootPosition,
					AttachmentsCount = 0,
					DocumentsPosition = docs.RootPosition,
					DocumentsCount = 0,
					DocumentsInTransactionPosition = docsInTx.RootPosition,
					DocumentsByIdPosition = docById.RootPosition,
					DocumentsByEtagPosition = docByEtag.RootPosition,
					TasksPosition = tasks.RootPosition,
					TasksCount = 0,
					TransactionPosition = tx.RootPosition,
					IdentityPosition = identity.RootPosition,
					IndexesPosition = indexes.RootPosition,
					QueuesPosition = queues.RootPosition,
					TasksByIndexPosition = tasksByIndex.RootPosition,
					MappedResultsByDocumentIdPosition = mapResultsByDocId.RootPosition,
					MappedResultsByReduceKeyPosition = mapResultsByReduceKey.RootPosition
				});
				writer.Flush(true);
			}
		}

		private void WriteTransaction(StorageTransaction tx)
		{
			var position = writer.Position;
			new JsonSerializer().Serialize(new BsonWriter(writer), tx);
			binaryWriter.Write(position);// this _have_ to be 8 bytes long, so we can read it from the end
			binaryWriter.Write(TransactionSignature);
		}


		public void Dispose()
		{
			disposed = true;
			writer.Dispose();
		}

		public void Write(Action<StorageActionsAccessor> func)
		{
			lock (locker)
			{
				using (var reader = OpenReader())
				{
					var mutator = new StorageMutator(writer, reader, transaction);
					var viewer = new StorageReader(reader, transaction);
					var accessor = new StorageActionsAccessor(mutator, viewer, reader, writer);
					func(accessor);
					mutator.Flush();
					var storageTransaction = mutator.CreateTransaction();
					if (transaction.Equals(storageTransaction))
						return;
					WriteTransaction(storageTransaction);
					writer.Flush(true);
					transaction = storageTransaction;
					accessor.RaiseCommitEvent();
					if (onCommit != null)
						onCommit();
				}
			}
		}

		public void Read(Action<StorageActionsAccessor> func)
		{
			using (var reader = OpenReader())
			{
				func(new StorageActionsAccessor(null, new StorageReader(reader, transaction), reader, null));
			}
		}


		internal class StorageTransaction
		{
			public long DocumentsInTransactionPosition { get; set; }
			public long DocumentsByIdPosition { get; set; }
			public long DocumentsByEtagPosition { get; set; }
			public long DocumentsPosition { get; set; }
			public long AttachmentPosition { get; set; }
			public long TasksPosition { get; set; }
			public long TasksCount { get; set; }
			public long DocumentsCount { get; set; }
			public long AttachmentsCount { get; set; }
			public long TransactionPosition { get; set; }
			public long IdentityPosition { get; set; }
			public long IndexesPosition { get; set; }
			public long QueuesPosition { get; set; }
			public long TasksByIndexPosition { get; set; }
			public long MappedResultsByReduceKeyPosition { get; set; }
			public long MappedResultsByDocumentIdPosition { get; set; }

			public bool Equals(StorageTransaction other)
			{
				if (ReferenceEquals(null, other)) return false;
				if (ReferenceEquals(this, other)) return true;
				return other.DocumentsInTransactionPosition == DocumentsInTransactionPosition &&
					other.DocumentsByIdPosition == DocumentsByIdPosition && other.DocumentsByEtagPosition == DocumentsByEtagPosition &&
						other.DocumentsPosition == DocumentsPosition && other.AttachmentPosition == AttachmentPosition &&
							other.TasksPosition == TasksPosition && other.TasksCount == TasksCount && other.DocumentsCount == DocumentsCount &&
								other.AttachmentsCount == AttachmentsCount && other.TransactionPosition == TransactionPosition &&
									other.IdentityPosition == IdentityPosition && other.IndexesPosition == IndexesPosition &&
										other.QueuesPosition == QueuesPosition && other.TasksByIndexPosition == TasksByIndexPosition &&
											other.MappedResultsByReduceKeyPosition == MappedResultsByReduceKeyPosition &&
												other.MappedResultsByDocumentIdPosition == MappedResultsByDocumentIdPosition;
			}

			public override bool Equals(object obj)
			{
				if (ReferenceEquals(null, obj)) return false;
				if (ReferenceEquals(this, obj)) return true;
				if (obj.GetType() != typeof (StorageTransaction)) return false;
				return Equals((StorageTransaction) obj);
			}

			public override int GetHashCode()
			{
				unchecked
				{
					int result = DocumentsInTransactionPosition.GetHashCode();
					result = (result*397) ^ DocumentsByIdPosition.GetHashCode();
					result = (result*397) ^ DocumentsByEtagPosition.GetHashCode();
					result = (result*397) ^ DocumentsPosition.GetHashCode();
					result = (result*397) ^ AttachmentPosition.GetHashCode();
					result = (result*397) ^ TasksPosition.GetHashCode();
					result = (result*397) ^ TasksCount.GetHashCode();
					result = (result*397) ^ DocumentsCount.GetHashCode();
					result = (result*397) ^ AttachmentsCount.GetHashCode();
					result = (result*397) ^ TransactionPosition.GetHashCode();
					result = (result*397) ^ IdentityPosition.GetHashCode();
					result = (result*397) ^ IndexesPosition.GetHashCode();
					result = (result*397) ^ QueuesPosition.GetHashCode();
					result = (result*397) ^ TasksByIndexPosition.GetHashCode();
					result = (result*397) ^ MappedResultsByReduceKeyPosition.GetHashCode();
					result = (result*397) ^ MappedResultsByDocumentIdPosition.GetHashCode();
					return result;
				}
			}
		}

		public Guid Id
		{
			get; private set;
		}

		public void Batch(Action<IStorageActionsAccessor> action)
		{
			if (disposed)
			{
				Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.");
				return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
			}
			if (current.Value != null)
			{
				action(current.Value);
				return;
			}
			try
			{
				Write(accessor =>
				{
					current.Value = accessor;
					action(accessor);
				});
			}
			finally
			{
				current.Value = null;
			}
		}

		public void ExecuteImmediatelyOrRegisterForSyncronization(Action action)
		{
			if (current.Value == null)
			{
				action();
				return;
			}
			current.Value.OnCommit += action;
		}

		public bool Initialize()
		{
			return isNewDatabase;
		}

		public void StartBackupOperation(Database.DocumentDatabase database, string backupDestinationDirectory)
		{
			throw new NotImplementedException();
		}

		public void Restore(string backupLocation, string databaseLocation)
		{
			throw new NotImplementedException();
		}
	}
}