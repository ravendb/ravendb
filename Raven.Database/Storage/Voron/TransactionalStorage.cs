namespace Raven.Database.Storage.Voron
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;
	using System.Threading;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Extensions;
	using Raven.Abstractions.MEF;
	using Raven.Database.Config;
	using Raven.Database.Impl;
	using Raven.Database.Impl.DTC;
	using Raven.Database.Plugins;
	using Raven.Database.Storage.Voron.Impl;

	using Storage;
	using Raven.Json.Linq;

	using global::Voron.Impl;

	public class TransactionalStorage : ITransactionalStorage
	{
		private readonly ThreadLocal<IStorageActionsAccessor> current = new ThreadLocal<IStorageActionsAccessor>();
		private readonly ThreadLocal<object> disableBatchNesting = new ThreadLocal<object>();

		private volatile bool disposed;
		private readonly DisposableAction exitLockDisposable;
		private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

		private OrderedPartCollection<AbstractDocumentCodec> documentCodecs;
		private readonly IDocumentCacher documentCacher;
		private IUuidGenerator uuidGenerator;

		private readonly InMemoryRavenConfiguration configuration;

		private readonly Action onCommit;

		private TableStorage tableStorage;

		public TransactionalStorage(InMemoryRavenConfiguration configuration, Action onCommit)
		{
			this.configuration = configuration;
			this.onCommit = onCommit;
			documentCacher = new DocumentCacher(configuration);
			exitLockDisposable = new DisposableAction(() => Monitor.Exit(this));
		}

		public void Dispose()
		{
			disposerLock.EnterWriteLock();
			try
			{
				if (disposed)
					return;
				disposed = true;
				current.Dispose();

				if (tableStorage != null)
					tableStorage.Dispose();
			}
			finally
			{
				disposerLock.ExitWriteLock();
			}
		}

		public Guid Id { get; private set; }

		public IDisposable WriteLock()
		{
			Monitor.Enter(this);
			return exitLockDisposable;
		}

		public IDisposable DisableBatchNesting()
		{
			disableBatchNesting.Value = new object();
			return new DisposableAction(() => disableBatchNesting.Value = null);
		}

		public void Batch(Action<IStorageActionsAccessor> action)
		{
			if (disposerLock.IsReadLockHeld && disableBatchNesting.Value == null) // we are currently in a nested Batch call and allow to nest batches
			{
				if (current.Value != null) // check again, just to be sure
				{
					current.Value.IsNested = true;
					action(current.Value);
					current.Value.IsNested = false;
					return;
				}
			}

			disposerLock.EnterReadLock();
			try
			{
				if (disposed)
				{
					Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.");
					return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
				}

				ExecuteBatch(action);
			}
			finally
			{
				disposerLock.ExitReadLock();
				if (disposed == false && disableBatchNesting.Value == null)
					current.Value = null;
			}

			onCommit(); // call user code after we exit the lock
		}

		private IStorageActionsAccessor ExecuteBatch(Action<IStorageActionsAccessor> action)
		{
			using (var snapshot = tableStorage.CreateSnapshot())
			using (var writeBatch = new WriteBatch())
			{
				var storageActionsAccessor = new StorageActionsAccessor(uuidGenerator, documentCodecs,
					documentCacher, writeBatch, snapshot, tableStorage);

				if (disableBatchNesting.Value == null)
					current.Value = storageActionsAccessor;

				action(storageActionsAccessor);
				storageActionsAccessor.SaveAllTasks();

				tableStorage.Write(writeBatch);
				return storageActionsAccessor;
			}
		}

		public void ExecuteImmediatelyOrRegisterForSynchronization(Action action)
		{
			throw new NotImplementedException();
		}

		public bool Initialize(IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs)
		{
			uuidGenerator = generator;
			this.documentCodecs = documentCodecs;

			var persistanceSource = configuration.RunInMemory ? (IPersistanceSource)new MemoryPersistanceSource() : new MemoryMapPersistanceSource(configuration);

			tableStorage = new TableStorage(persistanceSource);

			if (persistanceSource.CreatedNew)
			{
				Id = Guid.NewGuid();

				using (var writeIdBatch = new WriteBatch())
				{
					tableStorage.Details.AddOrUpdate(writeIdBatch, "id", Id.ToByteArray());
					tableStorage.Write(writeIdBatch);
				}
			}
			else
			{
				using (var snapshot = tableStorage.CreateSnapshot())
				{
					using (var read = tableStorage.Details.Read(snapshot, "id"))
					using (var reader = new BinaryReader(read.Stream))
					{
						Id = new Guid(reader.ReadBytes((int)read.Stream.Length));
					}
				}
			}

			return persistanceSource.CreatedNew;
		}

		public void StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory, bool incrementalBackup,
			DatabaseDocument documentDatabase)
		{
			throw new NotImplementedException();
		}

		public void Restore(string backupLocation, string databaseLocation, Action<string> output, bool defrag)
		{
			throw new NotImplementedException();
		}

		public long GetDatabaseSizeInBytes()
		{
			throw new NotImplementedException();
		}

		public long GetDatabaseCacheSizeInBytes()
		{
			return -1;
		}

		public long GetDatabaseTransactionVersionSizeInBytes()
		{
			return -1;
		}

		public string FriendlyName
		{
			get { return "Voron"; }
		}

		public bool HandleException(Exception exception)
		{
			return false;
		}

		public void Compact(InMemoryRavenConfiguration configuration)
		{
			throw new NotImplementedException();
		}

		public Guid ChangeId()
		{
			var newId = Guid.NewGuid();
			using (var changeIdWriteBatch = new WriteBatch())
			{
				tableStorage.Details.Delete(changeIdWriteBatch, "id");
				tableStorage.Details.AddOrUpdate(changeIdWriteBatch, "id", newId.ToByteArray());

				tableStorage.Write(changeIdWriteBatch);
			}

			Id = newId;
			return newId;
		}

		public void ClearCaches()
		{
		}

		public void DumpAllStorageTables()
		{
			throw new NotImplementedException();
		}

		public InFlightTransactionalState GetInFlightTransactionalState(Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> put, Func<string, Etag, TransactionInformation, bool> delete)
		{
			throw new NotImplementedException();
		}

		public IList<string> ComputeDetailedStorageInformation()
		{
			throw new NotImplementedException();
		}
	}
}
