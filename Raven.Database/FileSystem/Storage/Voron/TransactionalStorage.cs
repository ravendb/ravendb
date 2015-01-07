// -----------------------------------------------------------------------
//  <copyright file="TransactionalStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage.Voron.Backup;
using Raven.Database.FileSystem.Storage.Voron.Impl;
using Raven.Database.FileSystem.Storage.Voron.Schema;
using Raven.Json.Linq;

using Voron;
using Voron.Impl;
using Voron.Impl.Backup;

using Constants = Raven.Abstractions.Data.Constants;
using VoronExceptions = Voron.Exceptions;

namespace Raven.Database.FileSystem.Storage.Voron
{
    public class TransactionalStorage : ITransactionalStorage
    {
	    private readonly InMemoryRavenConfiguration configuration;

	    private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private readonly string path;

        private readonly NameValueCollection settings;

        private readonly ThreadLocal<IStorageActionsAccessor> current = new ThreadLocal<IStorageActionsAccessor>();

        private volatile bool disposed;

        private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        private readonly BufferPool bufferPool;

        private TableStorage tableStorage;

        private IdGenerator idGenerator;
	    private OrderedPartCollection<AbstractFileCodec> fileCodecs;

	    public TransactionalStorage(InMemoryRavenConfiguration configuration)
        {
	        this.configuration = configuration;
	        path = configuration.FileSystem.DataDirectory.ToFullPath();
	        settings = configuration.Settings;

            bufferPool = new BufferPool(2L * 1024 * 1024 * 1024, int.MaxValue); // 2GB max buffer size (voron limit)
        }

        public string FriendlyName
        {
            get { return "Voron"; }
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

        private static StorageEnvironmentOptions CreateStorageOptionsFromConfiguration(string path, NameValueCollection settings)
        {
            bool allowIncrementalBackupsSetting;
            if (bool.TryParse(settings[Constants.Voron.AllowIncrementalBackups] ?? "false", out allowIncrementalBackupsSetting) == false)
                throw new ArgumentException(Constants.Voron.AllowIncrementalBackups + " settings key contains invalid value");

            var directoryPath = path ?? AppDomain.CurrentDomain.BaseDirectory;
            var filePathFolder = new DirectoryInfo(directoryPath);
            if (filePathFolder.Exists == false)
                filePathFolder.Create();

            var tempPath = settings[Constants.Voron.TempPath];
            var journalPath = settings[Constants.RavenTxJournalPath];
            var options = StorageEnvironmentOptions.ForPath(directoryPath, tempPath, journalPath);
            options.IncrementalBackupEnabled = allowIncrementalBackupsSetting;
            return options;
        }

		public void Initialize(OrderedPartCollection<AbstractFileCodec> codecs)
        {
			if (codecs == null)
				throw new ArgumentNullException("codecs");

			fileCodecs = codecs;

            bool runInMemory;
            bool.TryParse(settings[Constants.RunInMemory], out runInMemory);

            var persistenceSource = runInMemory ? StorageEnvironmentOptions.CreateMemoryOnly() :
                CreateStorageOptionsFromConfiguration(path, settings);

            tableStorage = new TableStorage(persistenceSource, bufferPool);
	        var schemaCreator = new SchemaCreator(configuration, tableStorage, Output, Log);
			schemaCreator.CreateSchema();
			schemaCreator.SetupDatabaseIdAndSchemaVersion();
			schemaCreator.UpdateSchemaIfNecessary();

            SetupDatabaseId();
            idGenerator = new IdGenerator(tableStorage);
        }

        private void SetupDatabaseId()
        {
	        Id = tableStorage.Id;
        }

        public void Batch(Action<IStorageActionsAccessor> action)
        {
            if (Id == Guid.Empty)
                throw new InvalidOperationException("Cannot use Storage before Initialize was called");

            disposerLock.EnterReadLock();
            try
            {
                if (disposed)
                {
                    Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.\r\n" + new StackTrace(true));
                    return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
                }

                ExecuteBatch(action);
            }
            catch (Exception e)
            {
                if (disposed)
                {
					Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.\r\n" + e);
                    return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
                }

                if (e.InnerException is VoronExceptions.ConcurrencyException)
                    throw new ConcurrencyException("Concurrent modification to the same file are not allowed", e.InnerException);

                throw;
            }
            finally
            {
                disposerLock.ExitReadLock();
                try
                {
                    current.Value = null;
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        private void ExecuteBatch(Action<IStorageActionsAccessor> action)
        {
            if (current.Value != null)
            {
                action(current.Value);
                return;
            }

            using (var snapshot = tableStorage.CreateSnapshot())
            {
                var writeBatchRef = new Reference<WriteBatch>();
                try
                {
                    writeBatchRef.Value = new WriteBatch { DisposeAfterWrite = false };
                    using (var storageActionsAccessor = new StorageActionsAccessor(tableStorage, writeBatchRef, snapshot, idGenerator, bufferPool, fileCodecs))
                    {
                        current.Value = storageActionsAccessor;

                        action(storageActionsAccessor);
                        storageActionsAccessor.Commit();

                        tableStorage.Write(writeBatchRef.Value);
                    }
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

        public void StartBackupOperation(DocumentDatabase systemDatabase, RavenFileSystem filesystem, string backupDestinationDirectory, bool incrementalBackup,
            FileSystemDocument fileSystemDocument)
        {
            if (tableStorage == null)
                throw new InvalidOperationException("Cannot begin database backup - table store is not initialized");

            var backupOperation = new BackupOperation(filesystem, systemDatabase.Configuration.DataDirectory,
                backupDestinationDirectory, tableStorage.Environment, incrementalBackup, fileSystemDocument);

            Task.Factory.StartNew(() =>
            {
                using (backupOperation)
                {
                    backupOperation.Execute();
                }                    
            });
        }

        public void Restore(FilesystemRestoreRequest restoreRequest, Action<string> output)
        {
            new RestoreOperation(restoreRequest, configuration, output).Execute();
        }

        public void Compact(InMemoryRavenConfiguration configuration)
        {
            throw new NotSupportedException("Voron storage does not support compaction");
        }

        private void Output(string message)
		{
			Log.Info(message);
			Console.Write(message);
			Console.WriteLine();
		}
    }
}