// -----------------------------------------------------------------------
//  <copyright file="TransactionalStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage.Voron.Backup;
using Raven.Database.FileSystem.Storage.Voron.Impl;
using Raven.Database.FileSystem.Storage.Voron.Schema;
using Voron;
using Voron.Impl;
using Voron.Impl.Compaction;
using VoronConstants = Voron.Impl.Constants;
using Constants = Raven.Abstractions.Data.Constants;
using VoronExceptions = Voron.Exceptions;
using Raven.Abstractions.Threading;

namespace Raven.Database.FileSystem.Storage.Voron
{
    public class TransactionalStorage : ITransactionalStorage
    {
        private readonly InMemoryRavenConfiguration configuration;

        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private readonly string path;

        private readonly NameValueCollection settings;

        private readonly Raven.Abstractions.Threading.ThreadLocal<IStorageActionsAccessor> current = new Raven.Abstractions.Threading.ThreadLocal<IStorageActionsAccessor>();
        private readonly Raven.Abstractions.Threading.ThreadLocal<object> disableBatchNesting = new Raven.Abstractions.Threading.ThreadLocal<object>();

        private volatile bool disposed;

        private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        private readonly BufferPool bufferPool;

        private TableStorage tableStorage;

        private IdGenerator idGenerator;
        private OrderedPartCollection<AbstractFileCodec> fileCodecs;
        private UuidGenerator uuidGenerator;

        public TransactionalStorage(InMemoryRavenConfiguration configuration)
        {
            this.configuration = configuration;
            path = configuration.FileSystem.DataDirectory.ToFullPath();
            settings = configuration.Settings;

            RecoverFromFailedCompact(path);

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

        public void Initialize(UuidGenerator generator, OrderedPartCollection<AbstractFileCodec> codecs, Action<string> putResourceMarker = null)
        {
            if (codecs == null)
                throw new ArgumentNullException("codecs");

            fileCodecs = codecs;
            uuidGenerator = generator;

            bool runInMemory;
            bool.TryParse(settings[Constants.RunInMemory], out runInMemory);

            var persistenceSource = runInMemory ? StorageEnvironmentOptions.CreateMemoryOnly(settings[Constants.Voron.TempPath]) :
                CreateStorageOptionsFromConfiguration(path, settings);

            tableStorage = new TableStorage(persistenceSource, bufferPool);
            var schemaCreator = new SchemaCreator(configuration, tableStorage, Output, Log);
            schemaCreator.CreateSchema();
            schemaCreator.SetupDatabaseIdAndSchemaVersion();
            schemaCreator.UpdateSchemaIfNecessary();

            SetupDatabaseId();
            idGenerator = new IdGenerator(tableStorage);

            if (putResourceMarker != null)
                putResourceMarker(path);
        }

        private void SetupDatabaseId()
        {
            Id = tableStorage.Id;
        }

        public IDisposable DisableBatchNesting()
        {
            disableBatchNesting.Value = new object();
            return new DisposableAction(() => disableBatchNesting.Value = null);
        }

        public void Batch(Action<IStorageActionsAccessor> action)
        {
            if (Id == Guid.Empty)
                throw new InvalidOperationException("Cannot use Storage before Initialize was called");

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
                if (disposed == false && disableBatchNesting.Value == null)
                    current.Value = null;
            }
        }

        private void ExecuteBatch(Action<IStorageActionsAccessor> action)
        {
            var snapshotRef = new Reference<SnapshotReader>();
            var writeBatchRef = new Reference<WriteBatch>();

            var errorInUserAction = false;
            try
            {
                snapshotRef.Value = tableStorage.CreateSnapshot();
                writeBatchRef.Value = new WriteBatch { DisposeAfterWrite = false }; // prevent from disposing after write to allow read from batch OnStorageCommit
                var storageActionsAccessor = new StorageActionsAccessor(tableStorage, writeBatchRef, snapshotRef, idGenerator, bufferPool, uuidGenerator, fileCodecs);
                if (disableBatchNesting.Value == null)
                    current.Value = storageActionsAccessor;

                errorInUserAction = true;
                action(storageActionsAccessor);
                errorInUserAction = false;
                storageActionsAccessor.Commit();

                tableStorage.Write(writeBatchRef.Value);
            }
            catch (Exception e)
            {
                if (errorInUserAction == false)
                    Log.Error("Failed to execute transaction. Most likely something is really wrong here. Exception: " + e);
                throw;
            }
            finally
            {
                if (snapshotRef.Value != null)
                    snapshotRef.Value.Dispose();

                if (writeBatchRef.Value != null)
                    writeBatchRef.Value.Dispose();
            }
        }

        public Task StartBackupOperation(DocumentDatabase systemDatabase, RavenFileSystem filesystem, string backupDestinationDirectory, bool incrementalBackup,
            FileSystemDocument fileSystemDocument, ResourceBackupState state, CancellationToken token)
        {
            if (tableStorage == null)
                throw new InvalidOperationException("Cannot begin database backup - table store is not initialized");

            var backupOperation = new BackupOperation(filesystem, systemDatabase.Configuration.DataDirectory,
                backupDestinationDirectory, tableStorage.Environment, incrementalBackup, fileSystemDocument, state, token);

            return Task.Factory.StartNew(() =>
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

        public void Compact(InMemoryRavenConfiguration ravenConfiguration, Action<string> output)
        {
            bool runInMemory;
            bool.TryParse(settings[Constants.RunInMemory], out runInMemory);

            if (runInMemory)
                throw new InvalidOperationException("Cannot compact in-memory running Voron storage");

            tableStorage.Dispose();

            var sourcePath = path;
            var compactPath = Path.Combine(path, "Voron.Compaction");

            if (Directory.Exists(compactPath))
                Directory.Delete(compactPath, true);

            RecoverFromFailedCompact(sourcePath);

            var sourceOptions = CreateStorageOptionsFromConfiguration(path, settings);
            var compactOptions = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(compactPath);

            output("Executing storage compaction");
 
            StorageCompaction.Execute(sourceOptions, compactOptions,
                             x => output(string.Format("Copied {0} of {1} records in '{2}' tree. Copied {3} of {4} trees.", x.CopiedTreeRecords, x.TotalTreeRecordsCount, x.TreeName, x.CopiedTrees, x.TotalTreeCount)));

            var sourceDir = new DirectoryInfo(sourcePath);
            var sourceFiles = new List<FileInfo>();

            foreach (var pattern in new[] { "*.journal", "headers.one", "headers.two", VoronConstants.DatabaseFilename })
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

        private void Output(string message)
        {
            Log.Info(message);
            Console.Write(message);
            Console.WriteLine();
        }
    }
}
