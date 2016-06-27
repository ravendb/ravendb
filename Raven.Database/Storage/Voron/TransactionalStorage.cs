using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
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

using Sparrow.Collections;

using Voron;
using Voron.Impl;
using Voron.Impl.Compaction;
using VoronConstants = Voron.Impl.Constants;
using VoronExceptions = Voron.Exceptions;
using Task = System.Threading.Tasks.Task;
using Raven.Unix.Native;
using Raven.Abstractions;
using Raven.Abstractions.Threading;
using Raven.Database.Util;
using Voron.Impl.Paging;

namespace Raven.Storage.Voron
{
    public class TransactionalStorage : ITransactionalStorage
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private readonly ConcurrentSet<WeakReference<ITransactionalStorageNotificationHandler>> lowMemoryHandlers = new ConcurrentSet<WeakReference<ITransactionalStorageNotificationHandler>>();

        private readonly Raven.Abstractions.Threading.ThreadLocal<IStorageActionsAccessor> current = new Raven.Abstractions.Threading.ThreadLocal<IStorageActionsAccessor>();
        private readonly Raven.Abstractions.Threading.ThreadLocal<object> disableBatchNesting = new Raven.Abstractions.Threading.ThreadLocal<object>();

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
        private readonly Action onNestedTransactionExit;
        private readonly Action onNestedTransactionEnter;

        private Lazy<ConcurrentDictionary<int, RemainingReductionPerLevel>> scheduledReductionsPerViewAndLevel
            = new Lazy<ConcurrentDictionary<int, RemainingReductionPerLevel>>(() => new ConcurrentDictionary<int, RemainingReductionPerLevel>());

        public TransactionalStorage(InMemoryRavenConfiguration configuration, Action onCommit, Action onStorageInaccessible, Action onNestedTransactionEnter, Action onNestedTransactionExit)
        {
            this.configuration = configuration;
            this.onCommit = onCommit;
            this.onStorageInaccessible = onStorageInaccessible;

            RecoverFromFailedCompact(configuration.DataDirectory);

            documentCacher = CreateDocumentCacher(configuration);

            exitLockDisposable = new DisposableAction(() => Monitor.Exit(this));
            bufferPool = new BufferPool(
                configuration.Storage.Voron.MaxBufferPoolSize * 1024L * 1024L * 1024L, 
                int.MaxValue); // 2GB max buffer size (voron limit)
            this.onNestedTransactionEnter = onNestedTransactionEnter;
            this.onNestedTransactionExit = onNestedTransactionExit;
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

        public void DropAllIndexingInformation()
        {
            Batch(accessor =>
            {
                var schemaCreator = new SchemaCreator(configuration, tableStorage, Output, Log);
                var storage = schemaCreator.storage;
                using (var tx = storage.Environment.NewTransaction(TransactionFlags.ReadWrite))
                {
                    //deleting index related trees
                    storage.Environment.DeleteTree(tx, Tables.IndexingStats.TableName);
                    storage.Environment.DeleteTree(tx, Tables.LastIndexedEtags.TableName);
                    storage.Environment.DeleteTree(tx, Tables.DocumentReferences.TableName);
                    storage.Environment.DeleteTree(tx, storage.DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByRef));
                    storage.Environment.DeleteTree(tx, storage.DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByView));
                    storage.Environment.DeleteTree(tx, storage.DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByViewAndKey));
                    storage.Environment.DeleteTree(tx, storage.DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByKey));
                    storage.Environment.DeleteTree(tx, Tables.Tasks.TableName);
                    storage.Environment.DeleteTree(tx, storage.Tasks.GetIndexKey(Tables.Tasks.Indices.ByIndexAndType));
                    storage.Environment.DeleteTree(tx, storage.Tasks.GetIndexKey(Tables.Tasks.Indices.ByType));
                    storage.Environment.DeleteTree(tx, storage.Tasks.GetIndexKey(Tables.Tasks.Indices.ByIndex));
                    storage.Environment.DeleteTree(tx, Tables.ScheduledReductions.TableName);
                    storage.Environment.DeleteTree(tx, storage.ScheduledReductions.GetIndexKey(Tables.ScheduledReductions.Indices.ByView));
                    storage.Environment.DeleteTree(tx, storage.ScheduledReductions.GetIndexKey(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey));
                    storage.Environment.DeleteTree(tx, Tables.MappedResults.TableName);
                    storage.Environment.DeleteTree(tx, storage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByView));
                    storage.Environment.DeleteTree(tx, storage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByViewAndDocumentId));
                    storage.Environment.DeleteTree(tx, storage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByViewAndReduceKey));
                    storage.Environment.DeleteTree(tx, storage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket));
                    storage.Environment.DeleteTree(tx, storage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.Data));
                    storage.Environment.DeleteTree(tx, Tables.ReduceKeyCounts.TableName);
                    storage.Environment.DeleteTree(tx, storage.ReduceKeyCounts.GetIndexKey(Tables.ReduceKeyCounts.Indices.ByView));
                    storage.Environment.DeleteTree(tx, Tables.ReduceKeyTypes.TableName);
                    storage.Environment.DeleteTree(tx, storage.ReduceKeyTypes.GetIndexKey(Tables.ReduceKeyCounts.Indices.ByView));
                    storage.Environment.DeleteTree(tx, Tables.ReduceResults.TableName);
                    storage.Environment.DeleteTree(tx, storage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel));
                    storage.Environment.DeleteTree(tx, storage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket));
                    storage.Environment.DeleteTree(tx, storage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket));
                    storage.Environment.DeleteTree(tx, storage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByView));
                    storage.Environment.DeleteTree(tx, storage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.Data));
                    storage.Environment.DeleteTree(tx, Tables.ReduceStats.TableName);
                    storage.Environment.DeleteTree(tx, Tables.IndexingMetadata.TableName);
                    //creating the new empty indexes trees
                    SchemaCreator.CreateIndexingStatsSchema(tx, storage);
                    SchemaCreator.CreateLastIndexedEtagsSchema(tx, storage);
                    SchemaCreator.CreateDocumentReferencesSchema(tx, storage);
                    SchemaCreator.CreateTasksSchema(tx, storage);
                    SchemaCreator.CreateScheduledReductionsSchema(tx, storage);
                    SchemaCreator.CreateMappedResultsSchema(tx, storage);
                    SchemaCreator.CreateReduceKeyCountsSchema(tx, storage);
                    SchemaCreator.CreateReduceKeyTypesSchema(tx, storage);
                    SchemaCreator.CreateReduceResultsSchema(tx, storage);
                    SchemaCreator.CreateReduceStatsSchema(tx, storage);
                    SchemaCreator.CreateIndexingMetadataSchema(tx, storage);
                    tx.Commit();
                }
                accessor.Lists.RemoveAllOlderThan("Raven/Indexes/QueryTime", DateTime.MinValue);
                accessor.Lists.RemoveAllOlderThan("Raven/Indexes/PendingDeletion", DateTime.MinValue);
            });
        }
         
        public ConcurrentDictionary<int, RemainingReductionPerLevel> GetScheduledReductionsPerViewAndLevel()
        {
            return configuration.Indexing.DisableMapReduceInMemoryTracking?null: scheduledReductionsPerViewAndLevel.Value;
        }

        public void ResetScheduledReductionsTracking()
        {
            if (configuration.Indexing.DisableMapReduceInMemoryTracking) return;
            scheduledReductionsPerViewAndLevel = new Lazy<ConcurrentDictionary<int, RemainingReductionPerLevel>>(() => new ConcurrentDictionary<int, RemainingReductionPerLevel>());
        }

        public void RegisterTransactionalStorageNotificationHandler(ITransactionalStorageNotificationHandler handler)
        {
            lowMemoryHandlers.Add(new WeakReference<ITransactionalStorageNotificationHandler>(handler));
        }

        private void RunTransactionalStorageNotificationHandlers()
        {
            var inactiveHandlers = new List<WeakReference<ITransactionalStorageNotificationHandler>>();

            foreach (var lowMemoryHandler in lowMemoryHandlers)
            {
                ITransactionalStorageNotificationHandler handler;
                if (lowMemoryHandler.TryGetTarget(out handler))
                {
                    try
                    {
                        handler.HandleTransactionalStorageNotification();
                    }
                    catch (Exception e)
                    {
                        Log.Error("Failure to process transactional storage notification (handler - " + handler + ")", e);
                    }
                }
                else
                    inactiveHandlers.Add(lowMemoryHandler);
            }

            inactiveHandlers.ForEach(x => lowMemoryHandlers.TryRemove(x));
        }

        public Guid Id { get; private set; }
        public IDocumentCacher DocumentCacher { get { return documentCacher; }}

        public IDisposable WriteLock()
        {
            Monitor.Enter(this);
            return exitLockDisposable;
        }

        /// <summary>
        /// Force current operations inside context to be performed directly
        /// </summary>
        /// <returns></returns>
        public IDisposable DisableBatchNesting()
        {
            disableBatchNesting.Value = new object();
            if (onNestedTransactionEnter != null)
                onNestedTransactionEnter();

            return new DisposableAction(() =>
            {
                if (onNestedTransactionExit != null)
                    onNestedTransactionExit();
                disableBatchNesting.Value = null;
            });
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

        public bool SkipConsistencyCheck
        {
            get
            {
                return configuration.Storage.SkipConsistencyCheck;
            }
        }

        public void Batch(Action<IStorageActionsAccessor> action)
        {
            if (disposerLock.IsReadLockHeld && disableBatchNesting.Value == null) // we are currently in a nested Batch call and allow to nest batches
            {
                var storageActionsAccessor = current.Value;
                if (storageActionsAccessor != null) // check again, just to be sure
                {
                    storageActionsAccessor.IsNested = true;
                    action(storageActionsAccessor);
                    storageActionsAccessor.IsNested = false;
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
                    if (System.Environment.StackTrace.Contains(".Finalize()") == false)
                        throw;
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

            if (onCommit != null)
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

        public void Initialize(IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs, Action<string> putResourceMarker = null)
        {
            if (generator == null) throw new ArgumentNullException("generator");
            if (documentCodecs == null) throw new ArgumentNullException("documentCodecs");

            uuidGenerator = generator;
            _documentCodecs = documentCodecs;

            Log.Info("Starting to initialize Voron storage. Path: " + configuration.DataDirectory);

            StorageEnvironmentOptions options = configuration.RunInMemory ?
                CreateMemoryStorageOptionsFromConfiguration(configuration) :
                CreateStorageOptionsFromConfiguration(configuration);

            options.OnScratchBufferSizeChanged += size =>
            {
                if (configuration.Storage.Voron.ScratchBufferSizeNotificationThreshold < 0)
                    return;

                if (size < configuration.Storage.Voron.ScratchBufferSizeNotificationThreshold * 1024L * 1024L)
                    return;

                RunTransactionalStorageNotificationHandlers();
            };

            tableStorage = new TableStorage(options, bufferPool);
            var schemaCreator = new SchemaCreator(configuration, tableStorage, Output, Log);
            schemaCreator.CreateSchema();
            schemaCreator.SetupDatabaseIdAndSchemaVersion();
            if (!configuration.Storage.PreventSchemaUpdate)
                schemaCreator.UpdateSchemaIfNecessary();

            SetupDatabaseId();

            if (putResourceMarker != null)
                putResourceMarker(configuration.DataDirectory);

            Log.Info("Voron storage initialized");        }

        private void SetupDatabaseId()
        {
            Id = tableStorage.Id;
        }

        private static StorageEnvironmentOptions CreateMemoryStorageOptionsFromConfiguration(InMemoryRavenConfiguration configuration)
        {
            var options = StorageEnvironmentOptions.CreateMemoryOnly(configuration.Storage.Voron.TempPath);
            options.InitialFileSize = configuration.Storage.Voron.InitialFileSize;
            options.MaxScratchBufferSize = configuration.Storage.Voron.MaxScratchBufferSize * 1024L * 1024L;

            return options;
        }

        private static StorageEnvironmentOptions CreateStorageOptionsFromConfiguration(InMemoryRavenConfiguration configuration)
        {
            var directoryPath = configuration.DataDirectory ?? AppDomain.CurrentDomain.BaseDirectory;
            var filePathFolder = new DirectoryInfo (directoryPath);

            if (filePathFolder.Exists == false) {
                if (EnvironmentUtils.RunningOnPosix == true) {
                    uint permissions = 509;
                    Syscall.mkdir (filePathFolder.Name, permissions);
                }
                else
                    filePathFolder.Create ();
            }

            var tempPath = configuration.Storage.Voron.TempPath;
            var journalPath = configuration.Storage.Voron.JournalsStoragePath;
            var options = StorageEnvironmentOptions.ForPath(directoryPath, tempPath, journalPath);
            options.IncrementalBackupEnabled = configuration.Storage.Voron.AllowIncrementalBackups;
            options.InitialFileSize = configuration.Storage.Voron.InitialFileSize;
            options.MaxScratchBufferSize = configuration.Storage.Voron.MaxScratchBufferSize * 1024L * 1024L;

            return options;
        }

        public Task StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory, bool incrementalBackup,
            DatabaseDocument documentDatabase, ResourceBackupState state, CancellationToken cancellationToken)
        {
            if (tableStorage == null) 
                throw new InvalidOperationException("Cannot begin database backup - table store is not initialized");
            
            var backupOperation = new BackupOperation(database, database.Configuration.DataDirectory,
                backupDestinationDirectory, tableStorage.Environment, incrementalBackup, documentDatabase, state, cancellationToken);

            return Task.Factory.StartNew(() =>
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
            
            Log.Info("Starting to recover from failed compact. Data dir: " + sourcePath);

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

            Log.Info("Successfully recovered from failed compact");
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
            documentCacher = CreateDocumentCacher(configuration);
            oldDocumentCacher.Dispose();
        }

        public void DumpAllStorageTables()
        {
            throw new NotSupportedException("Not valid for Voron storage");
        }

        public StorageEnvironment Environment
        {
            get { return tableStorage.Environment; }
        }

        [CLSCompliant(false)]
        public InFlightTransactionalState InitializeInFlightTransactionalState(DocumentDatabase self, Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> put, Func<string, Etag, TransactionInformation, bool> delete)
        {            
            return new DtcNotSupportedTransactionalState(FriendlyName, put, delete);
        }

        public IList<string> ComputeDetailedStorageInformation(bool computeExactSizes, Action<string> progress, CancellationToken token)
        {
            var seperator = new String('#', 80);
            var padding = new String('\t', 1);
            var report = tableStorage.GenerateReportOnStorage(computeExactSizes, progress, token);
            var reportAsList = new List<string>();
            reportAsList.Add(string.Format("Total allocated db size: {0}", SizeHelper.Humane(report.DataFile.AllocatedSpaceInBytes)));
            reportAsList.Add(string.Format("Total used db size: {0}", SizeHelper.Humane(report.DataFile.SpaceInUseInBytes)));
            reportAsList.Add(string.Format("Total Trees Count: {0}", report.Trees.Count));
            reportAsList.Add("Trees:");
            foreach (var tree in report.Trees.OrderByDescending(x => x.PageCount))
            {
                var sb = new StringBuilder();
                sb.Append(System.Environment.NewLine);
                sb.Append(seperator);
                sb.Append(System.Environment.NewLine);
                sb.Append(padding);
                sb.Append(tree.Name);
                sb.Append(System.Environment.NewLine);
                sb.Append(seperator);
                sb.Append(System.Environment.NewLine);
                sb.Append("Owned Size: ");
                var ownedSize = AbstractPager.PageSize * tree.PageCount;
                sb.Append(SizeHelper.Humane(ownedSize));
                sb.Append(System.Environment.NewLine);
                if (computeExactSizes)
                {
                    sb.Append("Used Size: ");
                    sb.Append(SizeHelper.Humane((long)(ownedSize * tree.Density)));
                    sb.Append(System.Environment.NewLine);
                }
                sb.Append("Records: ");
                sb.Append(tree.EntriesCount);
                sb.Append(System.Environment.NewLine);
                sb.Append("Depth: ");
                sb.Append(tree.Depth);
                sb.Append(System.Environment.NewLine);
                sb.Append("PageCount: ");
                sb.Append(tree.PageCount);
                sb.Append(System.Environment.NewLine);
                sb.Append("LeafPages: ");
                sb.Append(tree.LeafPages);
                sb.Append(System.Environment.NewLine);
                sb.Append("BranchPages: ");
                sb.Append(tree.BranchPages);
                sb.Append(System.Environment.NewLine);
                sb.Append("OverflowPages: ");
                sb.Append(tree.OverflowPages);
                sb.Append(System.Environment.NewLine);

                if (tree.MultiValues != null)
                {
                    sb.Append("Multi values: ");
                    sb.Append(System.Environment.NewLine);

                    sb.Append(padding);
                    sb.Append("Records: ");
                    sb.Append(tree.MultiValues.EntriesCount);
                    sb.Append(System.Environment.NewLine);

                    sb.Append(padding);
                    sb.Append("PageCount: ");
                    sb.Append(tree.MultiValues.PageCount);
                    sb.Append(System.Environment.NewLine);

                    sb.Append(padding);
                    sb.Append("LeafPages: ");
                    sb.Append(tree.MultiValues.LeafPages);
                    sb.Append(System.Environment.NewLine);

                    sb.Append(padding);
                    sb.Append("BranchPages: ");
                    sb.Append(tree.MultiValues.BranchPages);
                    sb.Append(System.Environment.NewLine);

                    sb.Append(padding);
                    sb.Append("OverflowPages: ");
                    sb.Append(tree.MultiValues.OverflowPages);
                    sb.Append(System.Environment.NewLine);
                }

                reportAsList.Add(sb.ToString());
            }

            if (report.Journals.Any())
            {
                reportAsList.Add("Journals:");
                foreach (var journal in report.Journals.OrderByDescending(x => x.AllocatedSpaceInBytes))
                {
                    var sb = new StringBuilder();
                    sb.Append(System.Environment.NewLine);
                    sb.Append(seperator);
                    sb.Append(System.Environment.NewLine);
                    sb.Append(padding);
                    sb.Append("Journal number: ");
                    sb.Append(journal.Number);
                    sb.Append(System.Environment.NewLine);
                    sb.Append(seperator);
                    sb.Append(System.Environment.NewLine);
                    sb.Append("Allocated space: ");
                    sb.Append(SizeHelper.Humane(journal.AllocatedSpaceInBytes));
                    sb.Append(System.Environment.NewLine);

                    reportAsList.Add(sb.ToString());
                }
            }

            return reportAsList;
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

        private IDocumentCacher CreateDocumentCacher(InMemoryRavenConfiguration configuration)
        {
            if (configuration.CacheDocumentsInMemory == false)
                return new NullDocumentCacher();

            return new DocumentCacher(configuration);
        }
    }
}
