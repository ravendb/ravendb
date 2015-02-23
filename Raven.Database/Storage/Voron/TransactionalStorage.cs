using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util.Streams;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Impl.DTC;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Storage.Voron;
using Raven.Database.Storage.Voron.Backup;
using Raven.Database.Storage.Voron.Impl;
using Raven.Database.Storage.Voron.Schema;
using Raven.Json.Linq;

using Voron;
using Voron.Impl;
using Voron.Impl.Compaction;
using VoronConstants = Voron.Impl.Constants;
using VoronExceptions = Voron.Exceptions;
using Task = System.Threading.Tasks.Task;

namespace Raven.Storage.Voron
{
	public class TransactionalStorage : ITransactionalStorage
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

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
		private readonly Action onStorageInaccessible;

		private TableStorage tableStorage;

	    private readonly IBufferPool bufferPool;

		public TransactionalStorage(InMemoryRavenConfiguration configuration, Action onCommit, Action onStorageInaccessible)
		{
			this.configuration = configuration;
			this.onCommit = onCommit;
			this.onStorageInaccessible = onStorageInaccessible;

			RecoverFromFailedCompact(configuration.DataDirectory);

			documentCacher = new DocumentCacher(configuration);
			exitLockDisposable = new DisposableAction(() => Monitor.Exit(this));
            bufferPool = new BufferPool(
				configuration.Storage.Voron.MaxBufferPoolSize * 1024L * 1024L * 1024L, 
				int.MaxValue); // 2GB max buffer size (voron limit)
		}

		public void Dispose()
		{
			disposerLock.EnterWriteLock();
			try
			{
				if (disposed)
					return;

				disposed = true;

				var exceptionAggregator = new ExceptionAggregator("Could not properly dispose TransactionalStorage");

				exceptionAggregator.Execute(() => current.Dispose());

				if (tableStorage != null)
					exceptionAggregator.Execute(() => tableStorage.Dispose());

				if (bufferPool != null)
					exceptionAggregator.Execute(() => bufferPool.Dispose());

				exceptionAggregator.ThrowIfNeeded();
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
		    var snapshotReference = new Reference<SnapshotReader> { Value = tableStorage.CreateSnapshot() };
			var writeBatchReference = new Reference<WriteBatch> { Value = new WriteBatch() };
			
			var accessor = new StorageActionsAccessor(uuidGenerator, _documentCodecs,
                    documentCacher, writeBatchReference, snapshotReference, tableStorage, this, bufferPool);
			accessor.OnDispose += () =>
			{
				var exceptionAggregator = new ExceptionAggregator("Could not properly dispose StorageActionsAccessor");

				exceptionAggregator.Execute(() => snapshotReference.Value.Dispose());
				exceptionAggregator.Execute(() => writeBatchReference.Value.Dispose());

				exceptionAggregator.ThrowIfNeeded();
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

			Action afterStorageCommit;
			disposerLock.EnterReadLock();
			try
			{
				if (disposed)
				{
					Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.\r\n" + new StackTrace(true));
					return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
				}

				afterStorageCommit = ExecuteBatch(action);
			}
			catch (Exception e)
			{
				if (disposed)
				{
					Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.\r\n" + e);
					return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
				}

				if (e.InnerException is VoronExceptions.ConcurrencyException)
					throw new ConcurrencyException("Concurrent modification to the same document are not allowed", e.InnerException);

				if (e.InnerException is VoronExceptions.VoronUnrecoverableErrorException)
				{
					Trace.WriteLine("Voron has encountered unrecoverable error. The database will be disabled.\r\n" + e.InnerException);

					onStorageInaccessible();

					throw e.InnerException;
				}

				throw;
			}
			finally
			{
				disposerLock.ExitReadLock();
				if (disposed == false && disableBatchNesting.Value == null)
					current.Value = null;
			}

			if (afterStorageCommit != null)
				afterStorageCommit();

			onCommit(); // call user code after we exit the lock
		}

        private Action ExecuteBatch(Action<IStorageActionsAccessor> action)
        {
            var snapshotRef = new Reference<SnapshotReader>();
            var writeBatchRef = new Reference<WriteBatch>();
            try
            {
                snapshotRef.Value = tableStorage.CreateSnapshot();
                writeBatchRef.Value = new WriteBatch { DisposeAfterWrite = false }; // prevent from disposing after write to allow read from batch OnStorageCommit
                var storageActionsAccessor = new StorageActionsAccessor(uuidGenerator, _documentCodecs,
                                                                        documentCacher, writeBatchRef, snapshotRef,
                                                                        tableStorage, this, bufferPool);

                if (disableBatchNesting.Value == null)
                    current.Value = storageActionsAccessor;

                action(storageActionsAccessor);
                storageActionsAccessor.SaveAllTasks();
				storageActionsAccessor.ExecuteBeforeStorageCommit();

				tableStorage.Write(writeBatchRef.Value);

	            try
	            {
		            return storageActionsAccessor.ExecuteOnStorageCommit;
	            }
	            finally
	            {
					storageActionsAccessor.ExecuteAfterStorageCommit();
	            }
            }
            finally
            {
                if (snapshotRef.Value != null)
                    snapshotRef.Value.Dispose();

                if (writeBatchRef.Value != null)
                    writeBatchRef.Value.Dispose();
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

		public void Initialize(IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs)
		{
		    if (generator == null) throw new ArgumentNullException("generator");
		    if (documentCodecs == null) throw new ArgumentNullException("documentCodecs");

		    uuidGenerator = generator;
		    _documentCodecs = documentCodecs;

		    StorageEnvironmentOptions options = configuration.RunInMemory ?
				CreateMemoryStorageOptionsFromConfiguration(configuration) :
		        CreateStorageOptionsFromConfiguration(configuration);

		    tableStorage = new TableStorage(options, bufferPool);
			var schemaCreator = new SchemaCreator(configuration, tableStorage, Output, Log);
			schemaCreator.CreateSchema();
			schemaCreator.SetupDatabaseIdAndSchemaVersion();
			schemaCreator.UpdateSchemaIfNecessary();

		    SetupDatabaseId();
		}

	    private void SetupDatabaseId()
	    {
		    Id = tableStorage.Id;
	    }

		private static StorageEnvironmentOptions CreateMemoryStorageOptionsFromConfiguration(InMemoryRavenConfiguration configuration)
		{
			var options = StorageEnvironmentOptions.CreateMemoryOnly();
			options.InitialFileSize = configuration.Storage.Voron.InitialFileSize;
			options.MaxScratchBufferSize = configuration.Storage.Voron.MaxScratchBufferSize * 1024 * 1024;

			return options;
		}

	    private static StorageEnvironmentOptions CreateStorageOptionsFromConfiguration(InMemoryRavenConfiguration configuration)
        {
            var directoryPath = configuration.DataDirectory ?? AppDomain.CurrentDomain.BaseDirectory;
            var filePathFolder = new DirectoryInfo(directoryPath);
            if (filePathFolder.Exists == false)
                filePathFolder.Create();

		    var tempPath = configuration.Storage.Voron.TempPath;
		    var journalPath = configuration.Storage.Voron.JournalsStoragePath;
            var options = StorageEnvironmentOptions.ForPath(directoryPath, tempPath, journalPath);
            options.IncrementalBackupEnabled = configuration.Storage.Voron.AllowIncrementalBackups;
		    options.InitialFileSize = configuration.Storage.Voron.InitialFileSize;
		    options.MaxScratchBufferSize = configuration.Storage.Voron.MaxScratchBufferSize * 1024 * 1024;

            return options;
        }

		public void StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory, bool incrementalBackup,
			DatabaseDocument documentDatabase)
		{
			if (tableStorage == null) 
				throw new InvalidOperationException("Cannot begin database backup - table store is not initialized");
			
			var backupOperation = new BackupOperation(database, database.Configuration.DataDirectory,
		        backupDestinationDirectory, tableStorage.Environment, incrementalBackup, documentDatabase);

		    Task.Factory.StartNew(() =>
		    {
                using(backupOperation)
		            backupOperation.Execute();
		    });
		}       

		public void Restore(DatabaseRestoreRequest restoreRequest, Action<string> output)
		{
			new RestoreOperation(restoreRequest, configuration, output).Execute();
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

		public StorageStats GetStorageStats()
		{
			var stats = tableStorage.Environment.Stats();

			return new StorageStats()
			{
				VoronStats = new VoronStorageStats()
				{
					FreePagesOverhead = stats.FreePagesOverhead,
					RootPages = stats.RootPages,
					UnallocatedPagesAtEndOfFile = stats.UnallocatedPagesAtEndOfFile,
					UsedDataFileSizeInBytes = stats.UsedDataFileSizeInBytes,
					AllocatedDataFileSizeInBytes = stats.AllocatedDataFileSizeInBytes,
					NextWriteTransactionId = stats.NextWriteTransactionId,
					ActiveTransactions = stats.ActiveTransactions.Select(x => new VoronActiveTransaction
					{
						Id = x.Id,
						Flags = x.Flags.ToString()
					}).ToList()
				}
			};
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

	    public void Compact(InMemoryRavenConfiguration ravenConfiguration, Action<string> output)
	    {
			if (ravenConfiguration.RunInMemory)
				throw new InvalidOperationException("Cannot compact in-memory running Voron storage");

			tableStorage.Dispose();

			var sourcePath = ravenConfiguration.DataDirectory;
			var compactPath = Path.Combine(ravenConfiguration.DataDirectory, "Voron.Compaction");

			if (Directory.Exists(compactPath))
				Directory.Delete(compactPath, true);

			RecoverFromFailedCompact(sourcePath);

			var sourceOptions = CreateStorageOptionsFromConfiguration(ravenConfiguration);
		    var compactOptions = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions) StorageEnvironmentOptions.ForPath(compactPath);

	        output("Executing storage compaction");

	        StorageCompaction.Execute(sourceOptions, compactOptions,
	                                  x => output(string.Format("Copied {0} of {1} records in '{2}' tree. Copied {3} of {4} trees.", x.CopiedTreeRecords, x.TotalTreeRecordsCount, x.TreeName, x.CopiedTrees, x.TotalTreeCount)));
	        
		    var sourceDir = new DirectoryInfo(sourcePath);
		    var sourceFiles = new List<FileInfo>();
		    
		    foreach (var pattern in new [] { "*.journal", "headers.one", "headers.two", VoronConstants.DatabaseFilename})
		    {
			    sourceFiles.AddRange(sourceDir.GetFiles(pattern));
		    }

		    var compactionBackup = Path.Combine(sourcePath, "Voron.Compaction.Backup");

		    if (Directory.Exists(compactionBackup))
		    {
                Directory.Delete(compactionBackup, true);
		        output("Removing existing compaction backup directory");
		    }
			    

		    Directory.CreateDirectory(compactionBackup);

	        output("Backing up original data files");
		    foreach (var file in sourceFiles)
		    {
			    File.Move(file.FullName, Path.Combine(compactionBackup, file.Name));
		    }

		    var compactedFiles = new DirectoryInfo(compactPath).GetFiles();

	        output("Moving compacted files into target location");
			foreach (var file in compactedFiles)
			{
				File.Move(file.FullName, Path.Combine(sourcePath, file.Name));
			}

	        output("Deleting original data backup");

			Directory.Delete(compactionBackup, true);
			Directory.Delete(compactPath, true);
	    }

		private static void RecoverFromFailedCompact(string sourcePath)
		{
			var compactionBackup = Path.Combine(sourcePath, "Voron.Compaction.Backup");

			if (Directory.Exists(compactionBackup) == false) // not in the middle of compact op, we are good
				return;
			
			if (File.Exists(Path.Combine(sourcePath, VoronConstants.DatabaseFilename)) &&
				File.Exists(Path.Combine(sourcePath, "headers.one")) &&
				File.Exists(Path.Combine(sourcePath, "headers.two")) &&
				Directory.EnumerateFiles(sourcePath, "*.journal").Any() == false) // after a successful compaction there is no journal file
			{
				// we successfully moved new files and crashed before we could remove the old backup
				// just complete the op and we are good (committed)
				Directory.Delete(compactionBackup, true);
			}
			else
			{
				// just undo the op and we are good (rollback)

				var sourceDir = new DirectoryInfo(sourcePath);

				foreach (var pattern in new[] { "*.journal", "headers.one", "headers.two", VoronConstants.DatabaseFilename })
				{
					foreach (var file in sourceDir.GetFiles(pattern))
					{
						File.Delete(file.FullName);
					}
				}

				var backupFiles = new DirectoryInfo(compactionBackup).GetFiles();

				foreach (var file in backupFiles)
				{
					File.Move(file.FullName, Path.Combine(sourcePath, file.Name));
				}

				Directory.Delete(compactionBackup, true);
			}
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

		[CLSCompliant(false)]
		public InFlightTransactionalState GetInFlightTransactionalState(DocumentDatabase self, Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> put, Func<string, Etag, TransactionInformation, bool> delete)
		{            
		    return new DtcNotSupportedTransactionalState(FriendlyName, put, delete);
		}

		public IList<string> ComputeDetailedStorageInformation()
		{
		    return tableStorage.GenerateReportOnStorage()
		                       .Select(kvp => String.Format("{0} -> {1}", kvp.Key, kvp.Value))
		                       .ToList();
		}

		public List<TransactionContextData> GetPreparedTransactions()
		{
			throw new NotSupportedException("Voron storage does not support DTC");
		}

		public object GetInFlightTransactionsInternalStateForDebugOnly()
		{
			throw new NotSupportedException("Voron storage does not support DTC");
		}

		internal IStorageActionsAccessor GetCurrentBatch()
		{
			var batch = current.Value;
			if (batch == null)
				throw new InvalidOperationException("Batch was not started, you are not supposed to call this method");
			return batch;
		}

		private void Output(string message)
		{
			Log.Info(message);
			Console.Write(message);
			Console.WriteLine();
		}
	}
}
