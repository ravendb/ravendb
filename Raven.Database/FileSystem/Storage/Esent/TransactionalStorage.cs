//-----------------------------------------------------------------------
// <copyright file="TransactionalStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage.Esent.Backup;
using Raven.Database.FileSystem.Storage.Esent.Schema;
using Raven.Database.Util;
using BackupOperation = Raven.Database.FileSystem.Storage.Esent.Backup.BackupOperation;
using Raven.Abstractions.Threading;

namespace Raven.Database.FileSystem.Storage.Esent
{
    public class TransactionalStorage : CriticalFinalizerObject, ITransactionalStorage
    {
        private readonly InMemoryRavenConfiguration configuration;

        private OrderedPartCollection<AbstractFileCodec> fileCodecs;
        private readonly Raven.Abstractions.Threading.ThreadLocal<IStorageActionsAccessor> current = new Raven.Abstractions.Threading.ThreadLocal<IStorageActionsAccessor>();
        private readonly Raven.Abstractions.Threading.ThreadLocal<object> disableBatchNesting = new Raven.Abstractions.Threading.ThreadLocal<object>();
        private readonly string database;
        private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();
        private readonly string path;
        private readonly TableColumnsCache tableColumnsCache = new TableColumnsCache();
        private bool disposed;
        private readonly ILog log = LogManager.GetCurrentClassLogger();
        private JET_INSTANCE instance;
        private UuidGenerator uuidGenerator;

        private static readonly object UpdateLocker = new object();

        static TransactionalStorage()
        {
            try
            {
                SystemParameters.MaxInstances = 1024;
            }
            catch (EsentErrorException e)
            {
                if (e.Error != JET_err.AlreadyInitialized)
                    throw;
            }
        }

        public TransactionalStorage(InMemoryRavenConfiguration configuration)
        {
            configuration.Container.SatisfyImportsOnce(this);

            this.configuration = configuration;
            path = configuration.FileSystem.DataDirectory.ToFullPath();
            database = Path.Combine(path, "Data.ravenfs");

            RecoverFromFailedCompact(database);

            new TransactionalStorageConfigurator(configuration).LimitSystemCache();

            CreateInstance(out instance, database + Guid.NewGuid());
        }

        [ImportMany]
        public OrderedPartCollection<IFileSystemSchemaUpdate> Updaters { get; set; }

        public string FriendlyName
        {
            get { return "Esent"; }
        }

        public TableColumnsCache TableColumnsCache
        {
            get { return tableColumnsCache; }
        }

        public JET_INSTANCE Instance
        {
            get { return instance; }
        }

        public Guid Id { get; private set; }


        public void Dispose()
        {
            disposerLock.EnterWriteLock();
            try
            {
                if (disposed)
                    return;
                GC.SuppressFinalize(this);
                try
                {
                    Api.JetTerm2(instance, TermGrbit.Complete);
                }
                catch (Exception e)
                {
                    log.WarnException("Could not do gracefully disposal of RavenFS", e);
                    try
                    {
                        Api.JetTerm2(instance, TermGrbit.Abrupt);
                    }
                    catch (Exception e2)
                    {
                        log.FatalException("Even ungraceful shutdown was unsuccessful, restarting the server process may be required", e2);
                    }
                }
            }
            finally
            {
                disposed = true;
                disposerLock.ExitWriteLock();
            }
        }
        private static object locker = new object();
        public void Initialize(UuidGenerator generator, OrderedPartCollection<AbstractFileCodec> codecs, Action<string> putResourceMarker = null)
        {
            if(codecs == null)
                throw new ArgumentException("codecs");

            uuidGenerator = generator;
            fileCodecs = codecs;

            try
            {
                
                new TransactionalStorageConfigurator(configuration).ConfigureInstance(instance, path);
                bool lockTaken = false;
                //locking only the compaction didn't resolve the problem it seems this is the minimal amount of code that needs to be locked 
                //to prevent errors from esent
                try
                {
                    Monitor.TryEnter(locker, 30 * 1000, ref lockTaken);
                    if (lockTaken == false)
                    {
                        throw new TimeoutException("Couldn't take FS lock for initializing a new FS (Esent bug requires us to lock the storage), we have waited for 30 seconds, aborting.");
                    }
                    Api.JetInit(ref instance);

                    EnsureDatabaseIsCreatedAndAttachToDatabase();
                }
                finally
                {

                    if (lockTaken)
                        Monitor.Exit(locker);
                }
                SetIdFromDb();

                    tableColumnsCache.InitColumDictionaries(instance, database);

                    if (putResourceMarker != null)
                        putResourceMarker(path);
            }
            catch (Exception e)
            {
                Dispose();
                throw new InvalidOperationException("Could not open transactional storage: " + database, e);
            }
        }

        private void SetIdFromDb()
        {
            try
            {
                instance.WithDatabase(database, (session, dbid) =>
                {
                    using (var details = new Table(session, dbid, "details", OpenTableGrbit.ReadOnly))
                    {
                        Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
                        var columnids = Api.GetColumnDictionary(session, details);
                        var column = Api.RetrieveColumn(session, details, columnids["id"]);
                        Id = new Guid(column);
                        var schemaVersion = Api.RetrieveColumnAsString(session, details,
                                                                       columnids["schema_version"]);
                        if (schemaVersion == SchemaCreator.SchemaVersion)
                            return;

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

                                    updater.Value.Init(configuration);
                                    updater.Value.Update(session, dbid, Output);
                                    schemaVersion = Api.RetrieveColumnAsString(session, details, columnids["schema_version"]);

                                    ticker.Stop();

                                } while (schemaVersion != SchemaCreator.SchemaVersion);
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

        private bool EnsureDatabaseIsCreatedAndAttachToDatabase()
        {
            try
            {
                using (var session = new Session(instance))
                {
                    Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
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
                        CreateInstance(out instance, database + Guid.NewGuid());
                        Api.JetInit(ref instance);
                        using (var session = new Session(instance))
                        {
                            Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
                        }
                        return false;
                    case JET_err.DatabaseDirtyShutdown:
                        try
                        {
                            Api.JetTerm2(instance, TermGrbit.Complete);
                            using (var recoverInstance = new Instance("Recovery instance for: " + database))
                            {
                                new TransactionalStorageConfigurator(configuration).ConfigureInstance(recoverInstance.JetInstance, path);
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
                            log.WarnException("Could not recover from dirty shutdown in RavenFS " + database, e2);
                        }
                        CreateInstance(out instance, database + Guid.NewGuid());
                        Api.JetInit(ref instance);
                        using (var session = new Session(instance))
                        {
                            Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
                        }
                        return false;
                }
                if (e.Error != JET_err.FileNotFound)
                {
                    throw;
                }
            }

            using (var session = new Session(instance))
            {
                new SchemaCreator(session).Create(database);
                Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
                return true;
            }

        }

        ~TransactionalStorage()
        {
            try
            {
                Trace.WriteLine("Disposing esent resources from finalizer! You should call Storage.Dispose() instead!");
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

        [CLSCompliant(false)]
        public void Batch(Action<IStorageActionsAccessor> action)
        {
            var batchNestingAllowed = disableBatchNesting.Value == null;

            if (disposerLock.IsReadLockHeld && batchNestingAllowed) // we are currently in a nested Batch call and allow to nest batches
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
                ExecuteBatch(action);
            }
            catch (EsentErrorException e)
            {
                if (disposed)
                {
                    Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.\r\n" + e);
                    return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
                }

                switch (e.Error)
                {
                    case JET_err.WriteConflict:
                    case JET_err.SessionWriteConflict:
                    case JET_err.WriteConflictPrimaryIndex:
                        throw new ConcurrencyException("Concurrent modification to the same file are not allowed", e);
                    default:
                        throw;
                }
            }
            finally
            {
                disposerLock.ExitReadLock();
                if (disposed == false && disableBatchNesting.Value == null)
                    current.Value = null;
            }
        }

        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
        private void ExecuteBatch(Action<IStorageActionsAccessor> action)
        {
            var errorInUserAction = false;

            try
            {
                using (var storageActionsAccessor = new StorageActionsAccessor(tableColumnsCache, instance, database, uuidGenerator, fileCodecs))
                {
                    if (disableBatchNesting.Value == null)
                        current.Value = storageActionsAccessor;

                    errorInUserAction = true;
                    action(storageActionsAccessor);
                    errorInUserAction = false;
                    storageActionsAccessor.Commit();
                }
            }
            catch (Exception e)
            {
                if (errorInUserAction == false)
                    log.ErrorException("Failed to execute transaction. Most likely something is really wrong here.", e);

                throw;
            }
            finally
            {
                current.Value = null;
            }
        }

        public static void Compact(InMemoryRavenConfiguration ravenConfiguration, JET_PFNSTATUS statusCallback)
        {
            var src = Path.Combine(ravenConfiguration.FileSystem.DataDirectory.ToFullPath(), "Data.ravenfs");
            var compactPath = Path.Combine(ravenConfiguration.FileSystem.DataDirectory.ToFullPath(), "Data.ravenfs.Compact");

            if (File.Exists(compactPath))
                File.Delete(compactPath);
            RecoverFromFailedCompact(src);


            JET_INSTANCE compactInstance;
            CreateInstance(out compactInstance, ravenConfiguration.FileSystem.DataDirectory + Guid.NewGuid());
            try
            {
                new TransactionalStorageConfigurator(ravenConfiguration)
                    .ConfigureInstance(compactInstance, ravenConfiguration.FileSystem.DataDirectory);
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

        public static void CreateInstance(out JET_INSTANCE compactInstance, string name)
        {
            Api.JetCreateInstance(out compactInstance, name);

            DisableIndexChecking(compactInstance);
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

        public Task StartBackupOperation(DocumentDatabase systemDatabase, RavenFileSystem filesystem, string backupDestinationDirectory, bool incrementalBackup, 
            FileSystemDocument fileSystemDocument, ResourceBackupState state, CancellationToken token)
        {
            if (new InstanceParameters(instance).Recovery == false)
                throw new InvalidOperationException("Cannot start backup operation since the recovery option is disabled. In order to enable the recovery please set the RunInUnreliableYetFastModeThatIsNotSuitableForProduction configuration parameter value to false.");

            var backupOperation = new BackupOperation(filesystem, systemDatabase.Configuration.DataDirectory, backupDestinationDirectory, incrementalBackup, 
                fileSystemDocument, state, token);
            return Task.Factory.StartNew(backupOperation.Execute);
        }

        public void Restore(FilesystemRestoreRequest restoreRequest, Action<string> output)
        {
            new RestoreOperation(restoreRequest, configuration, output).Execute();
        }

        void ITransactionalStorage.Compact(InMemoryRavenConfiguration cfg, Action<string> output)
        {
            DateTime lastCompactionProgressStatusUpdate = DateTime.MinValue;

            Compact(cfg, (sesid, snp, snt, data) => {

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

        public IDisposable DisableBatchNesting()
        {
            disableBatchNesting.Value = new object();
            return new DisposableAction(() => disableBatchNesting.Value = null);
        }

        private void Output(string message)
        {
            Console.Write(message);
            Console.WriteLine();
        }
    }
}
