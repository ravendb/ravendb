//-----------------------------------------------------------------------
// <copyright file="TransactionalStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util;
using Raven.Database;
using Raven.Database.Commercial;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Impl.DTC;
using Raven.Database.Plugins;
using System.Linq;
using Raven.Database.Storage;
using Raven.Database.Storage.Esent;
using Raven.Database.Storage.Esent.Backup;
using Raven.Database.Storage.Esent.Debug;
using Raven.Database.Storage.Esent.StorageActions;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Storage.Esent.SchemaUpdates;

namespace Raven.Storage.Esent
{
    public class TransactionalStorage : CriticalFinalizerObject, ITransactionalStorage
    {
        private static int instanceCounter;
        private readonly ThreadLocal<StorageActionsAccessor> current = new ThreadLocal<StorageActionsAccessor>();
        private readonly ThreadLocal<object> disableBatchNesting = new ThreadLocal<object>();
        private readonly ThreadLocal<EsentTransactionContext> dtcTransactionContext = new ThreadLocal<EsentTransactionContext>();
        private readonly string database;
        private readonly InMemoryRavenConfiguration configuration;
        private readonly Action onCommit;
        private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        private readonly string path;
        private volatile bool disposed;

        private JET_INSTANCE instance;
        private readonly TableColumnsCache tableColumnsCache = new TableColumnsCache();
        private IUuidGenerator generator;
        private readonly IDocumentCacher documentCacher;
        private EsentInFlightTransactionalState inFlightTransactionalState;

        private static readonly ILog log = LogManager.GetCurrentClassLogger();
        private ConcurrentDictionary<int, RemainingReductionPerLevel> scheduledReductionsPerViewAndLevel = new ConcurrentDictionary<int, RemainingReductionPerLevel>();

        [ImportMany]
        public OrderedPartCollection<ISchemaUpdate> Updaters { get; set; }

        private static readonly object UpdateLocker = new object();

        static TransactionalStorage()
        {
            try
            {
                SystemParameters.MaxInstances = 1024;
            }
            catch (EsentErrorException e)
            {
                // this is expected if we had done something like recycling the app domain
                // because the engine state is actually at the process level (unmanaged)
                // so we ignore this error
                if (e.Error == JET_err.AlreadyInitialized)
                    return;
                throw;
            }
        }

        public TransactionalStorage(InMemoryRavenConfiguration configuration, Action onCommit, Action onStorageInaccessible, Action onNestedTransactionEnter, Action onNestedTransactionExit)
        {
            configuration.Container.SatisfyImportsOnce(this);

            if (configuration.CacheDocumentsInMemory == false)
            {
                documentCacher = new NullDocumentCacher();
            }
            else if (configuration.CustomMemoryCacher != null)
            {
                documentCacher = configuration.CustomMemoryCacher(configuration);
            }
            else
            {
                documentCacher = new DocumentCacher(configuration);
            }

            database = configuration.DataDirectory;
            this.configuration = configuration;
            this.onCommit = onCommit;
            
            path = database;
            if (Path.IsPathRooted(database) == false)
                path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, database);
            database = Path.Combine(path, "Data");

            RecoverFromFailedCompact(database);

            new TransactionalStorageConfigurator(configuration, this).LimitSystemCache();

            uniquePrefix = Interlocked.Increment(ref instanceCounter) + "-" + Base62Util.Base62Random();
            CreateInstance(out instance, uniquePrefix + "-" + database);
            this.onNestedTransactionEnter = onNestedTransactionEnter;
            this.onNestedTransactionExit = onNestedTransactionExit;
        }

        public ConcurrentDictionary<int, RemainingReductionPerLevel> GetScheduledReductionsPerViewAndLevel()
        {
            return configuration.Indexing.DisableMapReduceInMemoryTracking ? null : scheduledReductionsPerViewAndLevel;
        }

        public void ResetScheduledReductionsTracking()
        {
            scheduledReductionsPerViewAndLevel.Clear();
        }

        public void RegisterTransactionalStorageNotificationHandler(ITransactionalStorageNotificationHandler handler)
        {
        }

        public TableColumnsCache TableColumnsCache
        {
            get { return tableColumnsCache; }
        }

        public JET_INSTANCE Instance
        {
            get { return instance; }
        }

        public string Database
        {
            get { return database; }
        }

        public Guid Id { get; private set; }
        public IDocumentCacher DocumentCacher { get { return documentCacher; } }

        public void Dispose()
        {
            var tryEnterWriteLock = disposerLock.TryEnterWriteLock(TimeSpan.FromMinutes(2));
            try
            {
                if (tryEnterWriteLock == false)
                    log.Warn("After waiting for 2 minutes, could not acquire disposal lock, will force disposal anyway, pending transactions will all error");

                if (disposed)
                    return;

                var exceptionAggregator = new ExceptionAggregator(log, "Could not close database properly");
                disposed = true;
                exceptionAggregator.Execute(current.Dispose);
                if (documentCacher != null)
                    exceptionAggregator.Execute(documentCacher.Dispose);

                if (inFlightTransactionalState != null)
                    exceptionAggregator.Execute(inFlightTransactionalState.Dispose);

                exceptionAggregator.Execute(() =>
                    {
                        try
                        {
                            Api.JetTerm2(instance, TermGrbit.Complete);
                        }
                        catch (Exception e1)
                        {
                            log.ErrorException(
                                "Unexpected error occurred while terminating Esent Storage. Ignoring this error to allow to shutdown RavenDB instance.",
                                e1);

                            try
                            {
                                log.Warn(
                                    "Will now attempt to perform an abrupt shutdown, because asking nicely didn't work. You might need to run defrag on the database to recover potentially lost space (but no data will be lost).");
                                Api.JetTerm2(instance, TermGrbit.Abrupt);
                            }
                            catch (Exception e2)
                            {

                                log.FatalException(
                                    "Couldn't shut down the database server even when using abrupt, something is probably wrong and you'll need to restart the server process to access the database",
                                    e2);
                            }
                        }
                        finally
                        {
                            GC.SuppressFinalize(this);
                        }
                    });

                exceptionAggregator.Execute(current.Dispose);
                exceptionAggregator.Execute(disableBatchNesting.Dispose);
                exceptionAggregator.Execute(dtcTransactionContext.Dispose);

                exceptionAggregator.ThrowIfNeeded();
            }
            catch (Exception e)
            {
                log.FatalException("Could not dispose of the transactional storage for " + path, e);
                throw;
            }
            finally
            {
                if (tryEnterWriteLock)
                    disposerLock.ExitWriteLock();
            }
        }

        public Task StartBackupOperation(DocumentDatabase docDb, string backupDestinationDirectory, bool incrementalBackup, DatabaseDocument documentDatabase, ResourceBackupState state, CancellationToken cancellationToken)
        {
            if (new InstanceParameters(instance).Recovery == false)
                throw new InvalidOperationException("Cannot start backup operation since the recovery option is disabled. In order to enable the recovery please set the RunInUnreliableYetFastModeThatIsNotSuitableForProduction configuration parameter value to false.");

            var backupOperation = new BackupOperation(docDb, docDb.Configuration.DataDirectory, backupDestinationDirectory, incrementalBackup, documentDatabase, state, cancellationToken);
            return Task.Factory
                .StartNew(backupOperation.Execute);
        }

        public void Restore(DatabaseRestoreRequest restoreRequest, Action<string> output,InMemoryRavenConfiguration globalConfiguration)
        {
            new RestoreOperation(restoreRequest, configuration,globalConfiguration, output).Execute();
        }

        public DatabaseSizeInformation GetDatabaseSize()
        {
            long allocatedSizeInBytes;
            long usedSizeInBytes;

            using (var pht = new DocumentStorageActions(instance, database, tableColumnsCache, DocumentCodecs, generator, documentCacher, null, this))
            {
                int sizeInPages, pageSize, spaceOwnedInPages;
                Api.JetGetDatabaseInfo(pht.Session, pht.Dbid, out sizeInPages, JET_DbInfo.Filesize);
                Api.JetGetDatabaseInfo(pht.Session, pht.Dbid, out spaceOwnedInPages, JET_DbInfo.SpaceOwned);
                Api.JetGetDatabaseInfo(pht.Session, pht.Dbid, out pageSize, JET_DbInfo.PageSize);
                allocatedSizeInBytes = ((long)sizeInPages) * pageSize;
                usedSizeInBytes = ((long)spaceOwnedInPages) * pageSize;
            }

            return new DatabaseSizeInformation
                   {
                       AllocatedSizeInBytes = allocatedSizeInBytes,
                       UsedSizeInBytes = usedSizeInBytes
                   };
        }

        public long GetDatabaseCacheSizeInBytes()
        {
            return SystemParameters.CacheSize * SystemParameters.DatabasePageSize;
        }

        private long getDatabaseTransactionVersionSizeInBytesErrorValue;
        private bool reportedGetDatabaseTransactionCacheSizeInBytesError;
        private readonly string uniquePrefix;

        public long GetDatabaseTransactionVersionSizeInBytes()
        {
            if (getDatabaseTransactionVersionSizeInBytesErrorValue != 0)
                return getDatabaseTransactionVersionSizeInBytesErrorValue;

            try
            {
                const string categoryName = "Database ==> Instances";
                if (PerformanceCounterCategory.Exists(categoryName) == false)
                    return getDatabaseTransactionVersionSizeInBytesErrorValue = -1;
                var category = new PerformanceCounterCategory(categoryName);
                var instances = category.GetInstanceNames();
                var ravenInstance = instances.FirstOrDefault(x => x.Contains(uniquePrefix));
                const string counterName = "Version Buckets Allocated";
                if (ravenInstance == null || !category.CounterExists(counterName))
                {
                    return getDatabaseTransactionVersionSizeInBytesErrorValue = -2;
                }
                using (var counter = new PerformanceCounter(categoryName, counterName, ravenInstance, readOnly: true))
                {
                    var value = counter.NextValue();
                    return (long)(value * StorageConfigurator.GetVersionPageSize());
                }
            }
            catch (Exception e)
            {
                if (reportedGetDatabaseTransactionCacheSizeInBytesError == false)
                {
                    reportedGetDatabaseTransactionCacheSizeInBytesError = true;
                    log.WarnException("Failed to get Version Buckets Allocated value, this error will only be reported once.", e);
                }
                return getDatabaseTransactionVersionSizeInBytesErrorValue = -3;
            }
        }

        public StorageStats GetStorageStats()
        {
            return new StorageStats()
            {
                EsentStats = new EsentStorageStats()
            };
        }

        public string FriendlyName
        {
            get { return "Esent"; }
        }

        public bool HandleException(Exception exception)
        {
            var e = exception as EsentErrorException;
            if (e == null)
                return false;
            // we need to protect ourself from rollbacks happening in an async manner
            // after the database was already shut down.
            return e.Error == JET_err.InvalidInstance;
        }

        public bool SupportsDtc { get { return true; } }

        void ITransactionalStorage.Compact(InMemoryRavenConfiguration cfg, Action<string> output)
        {
            DateTime lastCompactionProgressStatusUpdate = DateTime.MinValue;

            Compact(cfg, (sesid, snp, snt, data) =>
            {

                if (snt == JET_SNT.Progress)
                {
                    if (SystemTime.UtcNow - lastCompactionProgressStatusUpdate < TimeSpan.FromMilliseconds(100))
                        return JET_err.Success;

                    lastCompactionProgressStatusUpdate = SystemTime.UtcNow;
                }

                output(string.Format("Esent Compact: {0} {1} {2}", snp, snt, data));
                return JET_err.Success;
            });
        }

        private static void RecoverFromFailedCompact(string file)
        {
            string renamedFile = file + ".RenameOp";
            if (File.Exists(renamedFile) == false) // not in the middle of compact op, we are good
                return;

            if (File.Exists(file))
            // we successfully renamed the new file and crashed before we could remove the old copy
            {
                //just complete the op and we are good (committed)
                File.Delete(renamedFile);
            }
            else // we successfully renamed the old file and crashed before we could remove the new file
            {
                // just undo the op and we are good (rollback)
                File.Move(renamedFile, file);
            }
        }


        private static object compactLocker = new object();
        private Action onNestedTransactionEnter;
        private Action onNestedTransactionExit;

        public static void Compact(InMemoryRavenConfiguration ravenConfiguration, JET_PFNSTATUS statusCallback)
        {
            bool lockTaken = false;
            try
            {
                Monitor.TryEnter(compactLocker, 30 * 1000, ref lockTaken);
                if (lockTaken == false)
                {
                    throw new TimeoutException("Could not take Esent Compact Lock. Because of a probable bug in Esent, we only allow a single database to be compacted at a given time.\r\n" +
                                               "However, we waited for 30 seconds for the compact lock to be released, and gave up. The database wasn't compacted, please try again later, when the other compaction process is over.");
                }
                CompactInternal(ravenConfiguration, statusCallback);
            }
            finally
            {

                if (lockTaken)
                    Monitor.Exit(compactLocker);
            }
        }

        private static void CompactInternal(InMemoryRavenConfiguration ravenConfiguration, JET_PFNSTATUS statusCallback)
        {
            var src = Path.Combine(ravenConfiguration.DataDirectory, "Data");
            var compactPath = Path.Combine(ravenConfiguration.DataDirectory, "Compacted.Data");

            if (File.Exists(compactPath))
                File.Delete(compactPath);
            RecoverFromFailedCompact(src);


            JET_INSTANCE compactInstance;
            CreateInstance(out compactInstance, ravenConfiguration.DataDirectory + Guid.NewGuid());
            try
            {
                new TransactionalStorageConfigurator(ravenConfiguration, null)
                    .ConfigureInstance(compactInstance, ravenConfiguration.DataDirectory);
                DisableIndexChecking(compactInstance);
                Api.JetInit(ref compactInstance);
                using (var session = new Session(compactInstance))
                {
                    Api.JetAttachDatabase(session, src, AttachDatabaseGrbit.None);
                    try
                    {
                        Api.JetCompact(session, src, compactPath, statusCallback, null,
                            CompactGrbit.None);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        throw;
                    }
                    finally
                    {
                        Api.JetDetachDatabase(session, src);
                    }
                }
            }
            finally
            {
                Api.JetTerm2(compactInstance, TermGrbit.Complete);
            }

            File.Move(src, src + ".RenameOp");
            File.Move(compactPath, src);
            File.Delete(src + ".RenameOp");

        }

        public static void CreateInstance(out JET_INSTANCE compactInstance, string name)
        {
            Api.JetCreateInstance(out compactInstance, name);

            DisableIndexChecking(compactInstance);
        }

        public Guid ChangeId()
        {
            Guid newId = Guid.NewGuid();
            instance.WithDatabase(database, (session, dbid, tx) =>
            {
                using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
                {
                    Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
                    var columnids = Api.GetColumnDictionary(session, details);
                    using (var update = new Update(session, details, JET_prep.Replace))
                    {
                        Api.SetColumn(session, details, columnids["id"], newId.ToByteArray());
                        update.Save();
                    }
                }
                return tx;
            });
            Id = newId;
            return newId;
        }

        public void DumpAllStorageTables()
        {
            Batch(accessor =>
            {
                var session = current.Value.Inner.Session;
                var jetDbid = current.Value.Inner.Dbid;
                foreach (var tableName in Api.GetTableNames(session, jetDbid))
                {
                    using (var table = new Table(session, jetDbid, tableName, OpenTableGrbit.ReadOnly))
                    using (var file = new FileStream(tableName + "-table.csv", FileMode.Create, FileAccess.ReadWrite, FileShare.None))
                    {
                        EsentUtil.DumpTable(session, table, file);
                    }
                }
            });
        }

        public IList<string> ComputeDetailedStorageInformation(bool computeExactSizes, Action<string> progress, CancellationToken token)
        {
            return StorageSizes.ReportOn(this, progress, token);
        }

        [CLSCompliant(false)]
        public InFlightTransactionalState InitializeInFlightTransactionalState(DocumentDatabase self, Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> put, Func<string, Etag, TransactionInformation, bool> delete)
        {
            var txMode = configuration.TransactionMode == TransactionMode.Lazy
               ? CommitTransactionGrbit.LazyFlush
               : CommitTransactionGrbit.None;

            return inFlightTransactionalState ?? (inFlightTransactionalState = new EsentInFlightTransactionalState(self, this, txMode, put, delete));
        }

        public void ClearCaches()
        {
            var cacheSizeMax = SystemParameters.CacheSizeMax;
            SystemParameters.CacheSize = 1; // force emptying of the cache
            SystemParameters.CacheSizeMax = 1;
            SystemParameters.CacheSize = 0;
            SystemParameters.CacheSizeMax = cacheSizeMax;
        }

        public void Initialize(IUuidGenerator uuidGenerator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs, Action<string> putResourceMarker = null, Action<object, Exception> onErrorAction = null)
        {
            try
            {
                DocumentCodecs = documentCodecs;
                generator = uuidGenerator;

                InstanceParameters instanceParameters = new TransactionalStorageConfigurator(configuration, this).ConfigureInstance(instance, path);

                if (configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction)
                    instanceParameters.Recovery = false;

                log.Info(@"Esent Settings:
  MaxVerPages      = {0}
  CacheSizeMax     = {1}
  DatabasePageSize = {2}", instanceParameters.MaxVerPages, SystemParameters.CacheSizeMax,
                         SystemParameters.DatabasePageSize);

                Api.JetInit(ref instance);

                EnsureDatabaseIsCreatedAndAttachToDatabase();

                SetIdFromDb();

                tableColumnsCache.InitColumDictionaries(instance, database);

                if (putResourceMarker != null)
                    putResourceMarker(path);
            }
            catch (Exception e)
            {
                onErrorAction?.Invoke(this, e);
                Dispose();
                var fileAccessException = e as EsentFileAccessDeniedException;
                if (fileAccessException == null)
                    throw new InvalidOperationException("Could not open transactional storage: " + database, e);
                throw new InvalidOperationException("Could not write to location: " + path + ". Make sure you have read/write permissions for this path.", e);
            }
        }

        protected OrderedPartCollection<AbstractDocumentCodec> DocumentCodecs { get; set; }

        public long MaxVerPagesValueInBytes { get; set; }

        private void SetIdFromDb()
        {
            try
            {
                instance.WithDatabase(database, (session, dbid, tx) =>
                {
                    using (var details = new Table(session, dbid, "details", OpenTableGrbit.ReadOnly))
                    {
                        Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
                        var columnids = Api.GetColumnDictionary(session, details);
                        var column = Api.RetrieveColumn(session, details, columnids["id"]);
                        Id = new Guid(column);
                        var schemaVersion = Api.RetrieveColumnAsString(session, details, columnids["schema_version"]);
                        if (configuration.Storage.PreventSchemaUpdate || schemaVersion == SchemaCreator.SchemaVersion)
                            return tx;

                        using (var ticker = new OutputTicker(TimeSpan.FromSeconds(3), () =>
                        {
                            log.Info(".");
                            Console.Write(".");
                        }, null, () =>
                        {
                            log.Info("OK");
                            Console.Write("OK");
                            Console.WriteLine();
                        }))
                        {
                            bool lockTaken = false;
                            try
                            {
                                Monitor.TryEnter(UpdateLocker, TimeSpan.FromSeconds(15), ref lockTaken);
                                if (lockTaken == false)
                                    throw new TimeoutException("Could not take upgrade lock after 15 seconds, probably another database is upgrading itself and we can't interrupt it midway. Please try again later");

                                do
                                {
                                    var updater = Updaters.FirstOrDefault(update => update.Value.FromSchemaVersion == schemaVersion);
                                    if (updater == null)
                                        throw new InvalidOperationException(
                                            string.Format(
                                                "The version on disk ({0}) is different that the version supported by this library: {1}{2}You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.",
                                                schemaVersion, SchemaCreator.SchemaVersion, Environment.NewLine));

                                    log.Info("Updating schema from version {0}: ", schemaVersion);
                                    Console.WriteLine("Updating schema from version {0}: ", schemaVersion);
                                    ticker.Start();

                                    updater.Value.Init(generator, configuration);
                                    updater.Value.Update(session, dbid, Output);

                                    tx.Commit(CommitTransactionGrbit.LazyFlush);
                                    tx = new Transaction(session);

                                    schemaVersion = Api.RetrieveColumnAsString(session, details, columnids["schema_version"]);

                                    ticker.Stop();

                                } while (schemaVersion != SchemaCreator.SchemaVersion);

                                return tx;
                            }
                            finally
                            {
                                if (lockTaken)
                                    Monitor.Exit(UpdateLocker);
                            }
                        }
                    }
                });
            }
            catch (EsentVersionStoreOutOfMemoryException esentOutOfMemoryException)
            {
                var message = "Schema Update Process for " + database + " Failed due to Esent Out Of Memory Exception!" +
                              Environment.NewLine +
                              "This might be caused by exceptionally large files, Consider enlarging the value of Raven/Esent/MaxVerPages (default:512)";
                log.Error(message);

                Console.WriteLine(message);
                throw new InvalidOperationException(message, esentOutOfMemoryException);
            }
            catch (Exception e)
            {
                var message = "Could not read db details from disk. It is likely that there is a version difference between the library and the db on the disk." +
                              Environment.NewLine +
                              "You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.";
                log.Error(message);
                Console.WriteLine(message);
                throw new InvalidOperationException(
                    message,
                    e);
            }
        }

        public void DropAllIndexingInformation()
        {
            Batch(accessor =>
            {
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);

                    try
                    {
                        foreach (var table in indexingDropTables)
                        {
                            Api.JetDeleteTable(session, dbid, table);
                        }

                        var schemaCreator = new SchemaCreator(session);
                        schemaCreator.CreateTasksTable(dbid);
                        schemaCreator.CreateScheduledReductionsTable(dbid);
                        schemaCreator.CreateMapResultsTable(dbid);
                        schemaCreator.CreateReduceResultsTable(dbid);
                        schemaCreator.CreateIndexingStatsTable(dbid);
                        schemaCreator.CreateIndexingStatsReduceTable(dbid);
                        schemaCreator.CreateIndexingEtagsTable(dbid);
                        schemaCreator.CreateReduceKeysCountsTable(dbid);
                        schemaCreator.CreateReduceKeysStatusTable(dbid);
                        schemaCreator.CreateIndexedDocumentsReferencesTable(dbid);
                    }
                    finally
                    {
                        Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                    }
                }

                accessor.Lists.RemoveAllOlderThan("Raven/Indexes/QueryTime", DateTime.MinValue);
                accessor.Lists.RemoveAllOlderThan("Raven/Indexes/PendingDeletion", DateTime.MinValue);
            });
        }

        private static readonly string[] indexingDropTables = {"tasks", "scheduled_reductions", "mapped_results", "reduce_results",
            "indexes_stats", "indexes_stats_reduce", "indexes_etag", "reduce_keys_counts", "reduce_keys_status", "indexed_documents_references"};

        private bool EnsureDatabaseIsCreatedAndAttachToDatabase()
        {

            int maxSize = 0;
            try
            {
                string value;
                if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("maxSizeInMb", out value))
                {
                    if (value != "unlimited")
                    {
                        maxSize = (int)((long.Parse(value) * 1024 * 1024) / SystemParameters.DatabasePageSize);
                    }
                }
                using (var session = new Session(instance))
                {
                    Api.JetAttachDatabase2(session, database, maxSize, AttachDatabaseGrbit.None);
                }
                return false;
            }
            catch (EsentErrorException e)
            {
                switch (e.Error)
                {
                    case JET_err.SecondaryIndexCorrupted:
                        Output("Secondary Index Corrupted detected, attempting to compact...");
                        Api.JetTerm2(instance, TermGrbit.Complete);
                        Compact(configuration, (sesid, snp, snt, data) =>
                        {
                            Output(string.Format("{0}, {1}, {2}, {3}", sesid, snp, snt, data));
                            return JET_err.Success;
                        });
                        CreateInstance(out instance, uniquePrefix + "-" + database);
                        Api.JetInit(ref instance);
                        using (var session = new Session(instance))
                        {
                            Api.JetAttachDatabase2(session, database, maxSize, AttachDatabaseGrbit.None);
                        }
                        return false;
                    case JET_err.DatabaseDirtyShutdown:
                        Output("Dirty shutdown detected, attempting to recover...");
                        try
                        {
                            Api.JetTerm2(instance, TermGrbit.Complete);
                            using (var recoverInstance = new Instance("Recovery instance for: " + database))
                            {
                                new TransactionalStorageConfigurator(configuration, this).ConfigureInstance(recoverInstance.JetInstance, path);
                                recoverInstance.Init();
                                using (var recoverSession = new Session(recoverInstance))
                                {
                                    Api.JetAttachDatabase(recoverSession, database,
                                                          AttachDatabaseGrbit.DeleteCorruptIndexes);
                                    Api.JetDetachDatabase(recoverSession, database);
                                }
                            }
                        }
                        catch (Exception e2)
                        {
                            log.WarnException("Could not recover database " + database + ", will try opening it one last time. If that doesn't work, try using esentutl", e2);
                        }
                        CreateInstance(out instance, uniquePrefix + "-" + database);
                        Api.JetInit(ref instance);
                        using (var session = new Session(instance))
                        {
                            Api.JetAttachDatabase2(session, database, maxSize, AttachDatabaseGrbit.None);
                        }
                        return false;
                }
                if (e.Error != JET_err.FileNotFound)
                    throw;
            }

            using (var session = new Session(instance))
            {
                new SchemaCreator(session).Create(database);
                Api.JetAttachDatabase2(session, database, maxSize, AttachDatabaseGrbit.None);
                return true;
            }
        }

        ~TransactionalStorage()
        {
            try
            {
                Trace.WriteLine(
                    "Disposing esent resources from finalizer! You should call TransactionalStorage.Dispose() instead!");
                Api.JetTerm2(instance, TermGrbit.Abrupt);
            }
            catch (Exception exception)
            {
                try
                {
                    Trace.WriteLine("Failed to dispose esent instance from finalizer because: " + exception);
                }
                catch
                {
                }
            }
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
                if (onNestedTransactionExit!= null)
                    onNestedTransactionExit();
                disableBatchNesting.Value = null;
            });
        }

        public IDisposable SetTransactionContext(EsentTransactionContext context)
        {
            dtcTransactionContext.Value = context;

            return new DisposableAction(() =>
            {
                dtcTransactionContext.Value = null;
            });
        }

        //[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
        [CLSCompliant(false)]
        public void Batch(Action<IStorageActionsAccessor> action)
        {
            var batchNestingAllowed = disableBatchNesting.Value == null;

            if (disposerLock.IsReadLockHeld && batchNestingAllowed) // we are currently in a nested Batch call and allow to nest batches
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
            Action afterStorageCommit = null;

            disposerLock.EnterReadLock();
            try
            {
                afterStorageCommit = ExecuteBatch(action, batchNestingAllowed ? dtcTransactionContext.Value : null);

                if (dtcTransactionContext.Value != null)
                {
                    dtcTransactionContext.Value.AfterCommit((Action) afterStorageCommit.Clone());
                    afterStorageCommit = null; // delay until transaction will be committed
                }
            }
            catch (Exception ex)
            {
                if (disposed)
                {
                    Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.\r\n" + ex);
                    if (Environment.StackTrace.Contains(".Finalize()") == false)
                        throw;
                    return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
                }

                EsentErrorException e = ex as EsentErrorException;
                if (e != null)
                {
                    switch (e.Error)
                    {
                        case JET_err.WriteConflict:
                        case JET_err.SessionWriteConflict:
                        case JET_err.WriteConflictPrimaryIndex:
                            throw new ConcurrencyException("Concurrent modification to the same document are not allowed", e);
                        default:
                            throw;
                    }
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

            if (onCommit!= null)
                onCommit(); // call user code after we exit the lock
            
            
        }

        //[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
        private Action ExecuteBatch(Action<IStorageActionsAccessor> action, EsentTransactionContext transactionContext)
        {
            var txMode = configuration.TransactionMode == TransactionMode.Lazy
                ? CommitTransactionGrbit.LazyFlush
                : CommitTransactionGrbit.None;

            bool lockTaken = false;
            if (transactionContext != null)
                Monitor.Enter(transactionContext, ref lockTaken);

            var errorInUserAction = false;
            try
            {
                using (var pht = new DocumentStorageActions(instance, database, tableColumnsCache, DocumentCodecs, generator, documentCacher, transactionContext, this))
                {
                    var dtcSnapshot = inFlightTransactionalState != null ? // might be not already initialized yet, during database creation
                                        inFlightTransactionalState.GetSnapshot() : EmptyInFlightStateSnapshot.Instance;

                    var storageActionsAccessor = new StorageActionsAccessor(pht, dtcSnapshot);
                    if (disableBatchNesting.Value == null)
                        current.Value = storageActionsAccessor;
                    errorInUserAction = true;
                    action(storageActionsAccessor);
                    errorInUserAction = false;
                    storageActionsAccessor.SaveAllTasks();
                    pht.ExecuteBeforeStorageCommit();

                    if (pht.UsingLazyCommit)
                        txMode = CommitTransactionGrbit.None;

                    try
                    {
                        return pht.Commit(txMode);
                    }
                    finally
                    {
                        pht.ExecuteAfterStorageCommit();
                    }
                }
            }
            catch (Exception e)
            {
                if(errorInUserAction == false)
                    log.ErrorException("Failed to execute transaction. Most likely something is really wrong here.", e);

                throw;
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(transactionContext);
            }
        }

        public IStorageActionsAccessor CreateAccessor()
        {
            var pht = new DocumentStorageActions(instance, database, tableColumnsCache, DocumentCodecs, generator,
                documentCacher, null, this);

            var accessor = new StorageActionsAccessor(pht, inFlightTransactionalState.GetSnapshot());

            accessor.OnDispose += pht.Dispose;

            return accessor;
        }

        public bool SkipConsistencyCheck
        {
            get
            {
                return configuration.Storage.SkipConsistencyCheck;
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

        public bool IsAlreadyInBatch
        {
            get { return current.Value != null; }
        }

        internal StorageActionsAccessor GetCurrentBatch()
        {
            var batch = current.Value;
            if (batch == null)
                throw new InvalidOperationException("Batch was not started, you are not supposed to call this method");
            return batch;
        }

        public static void DisableIndexChecking(JET_INSTANCE jetInstance)
        {
            Api.JetSetSystemParameter(jetInstance, JET_SESID.Nil, JET_param.EnableIndexChecking, 0, null);
            if (Environment.OSVersion.Version >= new Version(5, 2))
            {
                // JET_paramEnableIndexCleanup is not supported on WindowsXP

                const int JET_paramEnableIndexCleanup = 54;

                Api.JetSetSystemParameter(jetInstance, JET_SESID.Nil, (JET_param)JET_paramEnableIndexCleanup, 0, null);
            }
        }

        private void Output(string message)
        {
            log.Info(message);
            Console.Write("DB {0}: ", Path.GetFileName(Path.GetDirectoryName(database)));
            Console.Write(message);
            Console.WriteLine();
        }

        public List<TransactionContextData> GetPreparedTransactions()
        {
            return inFlightTransactionalState.GetPreparedTransactions();
        }

        public object GetInFlightTransactionsInternalStateForDebugOnly()
        {
            return inFlightTransactionalState.GetInFlightTransactionsInternalStateForDebugOnly();
        }
    }
}
