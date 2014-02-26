using System.Linq;
using Raven.Abstractions.Exceptions;
using Raven.Database;
using Raven.Database.Storage;
using Raven.Database.Storage.Voron;
using Raven.Database.Storage.Voron.Backup;
using Raven.Database.Util.Streams;

using VoronExceptions = Voron.Exceptions;
using Task = System.Threading.Tasks.Task;

namespace Raven.Storage.Voron
{
    using global::Voron.Impl;
    using Raven.Abstractions.Data;
    using Raven.Abstractions.Extensions;
    using Raven.Abstractions.MEF;
    using Raven.Database.Config;
    using Raven.Database.Impl;
    using Raven.Database.Impl.DTC;
    using Raven.Database.Plugins;
    using Raven.Database.Storage.Voron.Impl;
    using Raven.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;

	public class TransactionalStorage : ITransactionalStorage
	{
		private readonly ThreadLocal<IStorageActionsAccessor> current = new ThreadLocal<IStorageActionsAccessor>();
		private readonly ThreadLocal<object> disableBatchNesting = new ThreadLocal<object>();

		private volatile bool disposed;
		private readonly DisposableAction exitLockDisposable;
		private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

		private OrderedPartCollection<AbstractDocumentCodec> _documentCodecs;
		private IDocumentCacher documentCacher;
		private IUuidGenerator uuidGenerator;

		private readonly InMemoryRavenConfiguration configuration;

		private readonly Action onCommit;

		private TableStorage tableStorage;

	    private IBufferPool bufferPool;

		public TransactionalStorage(InMemoryRavenConfiguration configuration, Action onCommit)
		{
			this.configuration = configuration;
			this.onCommit = onCommit;
			documentCacher = new DocumentCacher(configuration);
			exitLockDisposable = new DisposableAction(() => Monitor.Exit(this));
            bufferPool = new BufferPool(configuration.VoronMaxBufferPoolSize * 1024 * 1024 * 1024, int.MaxValue); // 2GB max buffer size (voron limit)
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

                if (bufferPool != null)
                    bufferPool.Dispose();
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

		public IStorageActionsAccessor CreateAccessor()
		{
			var snapshot = tableStorage.CreateSnapshot();
			var writeBatchReference = new Reference<WriteBatch> { Value = new WriteBatch() };
			
			var accessor = new StorageActionsAccessor(uuidGenerator, _documentCodecs,
                    documentCacher, writeBatchReference, snapshot, tableStorage, this, bufferPool);
			accessor.OnDispose += () =>
			{
				snapshot.Dispose();
                writeBatchReference.Value.Dispose();
			};

			return accessor;
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
			catch (Exception e)
			{
				if (disposed)
				{
					Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.");
					return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
				}

				if (e.InnerException is VoronExceptions.ConcurrencyException)
					throw new ConcurrencyException("Concurrent modification to the same document are not allowed", e.InnerException);

				throw;
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
			{
			    var writeBatchRef = new Reference<WriteBatch>();
                try
                {
                    writeBatchRef.Value = new WriteBatch() {DisposeAfterWrite = false};// prevent from disposing after write to allow read from batch OnStorageCommit
                    var storageActionsAccessor = new StorageActionsAccessor(uuidGenerator, _documentCodecs,
                                                                            documentCacher, writeBatchRef, snapshot,
                                                                            tableStorage, this, bufferPool);

                    if (disableBatchNesting.Value == null)
                        current.Value = storageActionsAccessor;

                    action(storageActionsAccessor);
                    storageActionsAccessor.SaveAllTasks();

                    tableStorage.Write(writeBatchRef.Value);

                    storageActionsAccessor.ExecuteOnStorageCommit();

                    return storageActionsAccessor;
                }
                finally
                {
                    if (writeBatchRef.Value != null)
                    {
                        writeBatchRef.Value.Dispose();
                    }
                }
			   
			}
		}

		public void ExecuteImmediatelyOrRegisterForSynchronization(Action action)
		{
            if (current.Value == null)
            {
                action();
                return;
            }
            current.Value.OnStorageCommit += action;
        }

		public bool Initialize(IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs)
		{
		    if (generator == null) throw new ArgumentNullException("generator");
		    if (documentCodecs == null) throw new ArgumentNullException("documentCodecs");

		    uuidGenerator = generator;
			_documentCodecs = documentCodecs;

			var persistenceSource = configuration.RunInMemory ? (IPersistenceSource)new MemoryPersistenceSource() : new MemoryMapPersistenceSource(configuration);

			tableStorage = new TableStorage(persistenceSource, bufferPool);

			if (persistenceSource.CreatedNew)
			{
				Id = Guid.NewGuid();

				using (var writeIdBatch = new WriteBatch())
				{
					tableStorage.Details.Add(writeIdBatch, "id", Id.ToByteArray());
					tableStorage.Write(writeIdBatch);
				}
			}
			else
			{
				using (var snapshot = tableStorage.CreateSnapshot())
				{
					var read = tableStorage.Details.Read(snapshot, "id", null);
					if (read == null || read.Reader == null || read.Reader.Length == 0) //precaution - might prevent NRE in edge cases
						throw new InvalidDataException("Failed to initialize Voron transactional storage. Possible data corruption.");

					using (var stream = read.Reader.AsStream())
					using (var reader = new BinaryReader(stream))
					{
						Id = new Guid(reader.ReadBytes((int) stream.Length));
					}
				}
			}

			return persistenceSource.CreatedNew;
		}

		public void StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory, bool incrementalBackup,
			DatabaseDocument documentDatabase)
		{
			if (tableStorage == null) 
				throw new InvalidOperationException("Cannot begin database backup - table store is not initialized");
			
			var backupOperation = new BackupOperation(database, database.Configuration.DataDirectory,
		        backupDestinationDirectory, tableStorage.Environment, incrementalBackup,documentDatabase);
			
            Task.Factory.StartNew(backupOperation.Execute);
		}       

		public void Restore(string backupLocation, string databaseLocation, Action<string> output, bool defrag)
		{
			new RestoreOperation(backupLocation, configuration, output).Execute();
		}

	    public DatabaseSizeInformation GetDatabaseSize()
	    {
	        var stats = tableStorage.Environment.Stats();

            return new DatabaseSizeInformation
                   {
                       AllocatedSizeInBytes = stats.AllocatedDataFileSizeInBytes,
                       UsedSizeInBytes = stats.UsedDataFileSizeInBytes
                   };
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
			return false; //false returned --> all exceptions (if any) are properly rethrown in DocumentDatabase
		}

		public bool IsAlreadyInBatch
		{
			get
			{
				return current.Value != null;
			}
		}
        public bool SupportsDtc { get { return false; } }

	    public void Compact(InMemoryRavenConfiguration configuration)
		{
			//Voron storage does not support compaction
		}

		public Guid ChangeId()
		{
			var newId = Guid.NewGuid();
			using (var changeIdWriteBatch = new WriteBatch())
			{
				tableStorage.Details.Delete(changeIdWriteBatch, "id");
				tableStorage.Details.Add(changeIdWriteBatch, "id", newId.ToByteArray());

				tableStorage.Write(changeIdWriteBatch);
			}

			Id = newId;
			return newId;
		}

		public void ClearCaches()
		{
		    var oldDocumentCacher = documentCacher;
		    documentCacher = new DocumentCacher(configuration);
		    oldDocumentCacher.Dispose();
		}

		public void DumpAllStorageTables()
		{
            throw new NotSupportedException("Not valid for Voron storage");
        }

		public InFlightTransactionalState GetInFlightTransactionalState(Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> put, Func<string, Etag, TransactionInformation, bool> delete)
		{            
		    return new DtcNotSupportedTransactionalState(FriendlyName, put, delete);
		}

		public IList<string> ComputeDetailedStorageInformation()
		{
		    return tableStorage.GenerateReportOnStorage()
		                       .Select(kvp => String.Format("{0} -> {1}", kvp.Key, kvp.Value))
		                       .ToList();
		}

		internal IStorageActionsAccessor GetCurrentBatch()
		{
			var batch = current.Value;
			if (batch == null)
				throw new InvalidOperationException("Batch was not started, you are not supposed to call this method");
			return batch;
		}
	}
}
