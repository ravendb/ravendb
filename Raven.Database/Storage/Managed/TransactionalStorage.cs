//-----------------------------------------------------------------------
// <copyright file="TransactionalStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Impl.DTC;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Munin;
using Raven.Storage.Managed.Backup;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
	public class TransactionalStorage : ITransactionalStorage
	{
		private readonly ThreadLocal<IStorageActionsAccessor> current = new ThreadLocal<IStorageActionsAccessor>();
		private readonly ThreadLocal<object> disableBatchNesting = new ThreadLocal<object>();

		private readonly InMemoryRavenConfiguration configuration;
		private readonly Action onCommit;
		private TableStorage tableStorage;

		private OrderedPartCollection<AbstractDocumentCodec> DocumentCodecs { get; set; }
		public TableStorage TableStorage
		{
			get { return tableStorage; }
		}

		private IPersistentSource persistenceSource;
		private volatile bool disposed;
		private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
		private Timer idleTimer;
		private long lastUsageTime;
		private IUuidGenerator uuidGenerator;
		private readonly IDocumentCacher documentCacher;
		private MuninInFlightTransactionalState inFlightTransactionalState;

	    public IPersistentSource PersistenceSource
		{
			get { return persistenceSource; }
		}

		public TransactionalStorage(InMemoryRavenConfiguration configuration, Action onCommit)
		{
			this.configuration = configuration;
			this.onCommit = onCommit;
		//	IDocumentCacher documentCacher = null;
			if (configuration.UseNullDocumentCacher)
			{
				documentCacher = new NullDocumentCacher();
			}
			else
			{
				documentCacher = new DocumentCacher(configuration);
			}
			
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
				if (documentCacher != null)
					documentCacher.Dispose();
				if (idleTimer != null)
					idleTimer.Dispose();
				if (persistenceSource != null)
					persistenceSource.Dispose();
				if (tableStorage != null)
					tableStorage.Dispose();
			}
			finally
			{
				disposerLock.ExitWriteLock();
			}
		}

		public Guid Id
		{
			get;
			private set;
		}

		public IDisposable DisableBatchNesting()
		{
			// Removed Munin support in disabling bath nesting due to munin concurrency issues

			/*disableBatchNesting.Value = new object();
		    var old = tableStorage.CurrentTransactionId.Value;
            tableStorage.CurrentTransactionId.Value = Guid.Empty;*/
			return new DisposableAction(() =>
			{
			    /*tableStorage.CurrentTransactionId.Value = old;
			    disableBatchNesting.Value = null;*/
			});
		}

		[DebuggerNonUserCode]
		public void Batch(Action<IStorageActionsAccessor> action)
		{
			if (disposerLock.IsReadLockHeld && disableBatchNesting.Value == null) // we are currently in a nested Batch call and allow to nest batches
			{
				if (current.Value != null) // check again, just to be sure
				{
				    var old = current.Value.IsNested;
                    current.Value.IsNested = true;
				    try
				    {
                        action(current.Value);
				    }
				    finally
				    {
                        current.Value.IsNested = old;
				    }
					return;
				}
			}
			StorageActionsAccessor result;
			disposerLock.EnterReadLock();
			try
			{
				if (disposed)
				{
					Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.");
					return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
				}

				result = ExecuteBatch(action);
			}
			finally
			{
				disposerLock.ExitReadLock();
				if (disposed == false && disableBatchNesting.Value == null)
					current.Value = null;
			}
			result.InvokeOnCommit();
			onCommit(); // call user code after we exit the lock
		}

		[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		private StorageActionsAccessor ExecuteBatch(Action<IStorageActionsAccessor> action)
		{
			Interlocked.Exchange(ref lastUsageTime, SystemTime.UtcNow.ToBinary());
			using (tableStorage.BeginTransaction())
			{
				var storageActionsAccessor = new StorageActionsAccessor(tableStorage, uuidGenerator, DocumentCodecs, documentCacher);
				if (disableBatchNesting.Value == null)
					current.Value = storageActionsAccessor;
				action(storageActionsAccessor);
				storageActionsAccessor.SaveAllTasks();
				tableStorage.Commit();
				return storageActionsAccessor;
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
			DocumentCodecs = documentCodecs;
			uuidGenerator = generator;
			if (configuration.RunInMemory  == false && Directory.Exists(configuration.DataDirectory) == false)
				Directory.CreateDirectory(configuration.DataDirectory);

			persistenceSource = configuration.RunInMemory
						  ? (IPersistentSource)new MemoryPersistentSource()
						  : new FileBasedPersistentSource(configuration.DataDirectory, "Raven", configuration.TransactionMode == TransactionMode.Safe);

			tableStorage = new TableStorage(persistenceSource);

			idleTimer = new Timer(MaybeOnIdle, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

			tableStorage.Initialize();

			if (persistenceSource.CreatedNew)
			{
				Id = Guid.NewGuid();
				Batch(accessor => tableStorage.Details.Put("id", Id.ToByteArray()));
			}
			else
			{
				using(tableStorage.BeginTransaction())
				{
				var readResult = tableStorage.Details.Read("id");
				Id = new Guid(readResult.Data());
			}
			}

			return persistenceSource.CreatedNew;
		}

		public void StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory, bool incrementalBackup, DatabaseDocument databaseDocument)
		{
			if (configuration.RunInMemory)
				throw new InvalidOperationException("Backup operation is not supported when running in memory. In order to enable backup operation please make sure that you persistent the data to disk by setting the RunInMemory configuration parameter value to false.");

			var backupOperation = new BackupOperation(database, persistenceSource, database.Configuration.DataDirectory, backupDestinationDirectory, databaseDocument);
			Task.Factory.StartNew(backupOperation.Execute);
		}

		public void Restore(string backupLocation, string databaseLocation, Action<string> output, bool defrag)
		{
			new RestoreOperation(backupLocation, databaseLocation, output).Execute();
		}

		public long GetDatabaseSizeInBytes()
		{
			return PersistenceSource.Read(stream => stream.Length);
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
			get { return "Munin"; }
		}

		public bool HandleException(Exception exception)
		{
			return false;
		}

		public bool IsAlreadyInBatch
		{
			get { return current.Value != null; }
		}

		public void Compact(InMemoryRavenConfiguration compactConfiguration)
		{
			using (var ps = new FileBasedPersistentSource(compactConfiguration.DataDirectory, "Raven", configuration.TransactionMode == TransactionMode.Safe))
			using (var storage = new TableStorage(ps))
			{
				storage.Compact();
			}

		}

		public Guid ChangeId()
		{
			Guid newId = Guid.NewGuid();
			Batch(accessor =>
			{
				tableStorage.Details.Remove("id");
				tableStorage.Details.Put("id", newId.ToByteArray());
			});
			Id = newId;
			return newId;
		}

		public void DumpAllStorageTables()
		{
			throw new NotSupportedException("Not valid for munin");
		}

        public IList<string> ComputeDetailedStorageInformation()
        {
            return new List<string>
            {
                "Detailed storage sizes is not available for Munin"
            };
        }

	    public InFlightTransactionalState GetInFlightTransactionalState(DocumentDatabase self, Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> put, Func<string, Etag, TransactionInformation, bool> delete)
		{
			return inFlightTransactionalState ?? (inFlightTransactionalState = new MuninInFlightTransactionalState(this, put, delete));
		}

		public void ClearCaches()
		{
			// don't do anything here
		}

		private void MaybeOnIdle(object _)
		{
			var ticks = Interlocked.Read(ref lastUsageTime);
			var lastUsage = DateTime.FromBinary(ticks);
			if ((SystemTime.UtcNow - lastUsage).TotalSeconds < 30)
				return;

			if (disposed)
				return;

			tableStorage.PerformIdleTasks();
		}

		public void EnsureCapacity(int value)
		{
			persistenceSource.EnsureCapacity(value);
		}


        public List<TransactionContextData> GetPreparedTransactions()
        {
            return new List<TransactionContextData>();
        }


		public object GetInFlightTransactionsInternalStateForDebugOnly()
		{
			return inFlightTransactionalState.GetInFlightTransactionsInternalStateForDebugOnly();
		}

	    public void DropAllIndexingInformation()
	    {
	        throw new NotSupportedException("Drop all indexes isn't supported for this storage");
	    }
	}
}
