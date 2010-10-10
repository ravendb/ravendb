using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Storage;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
    public class TransactionalStorage : ITransactionalStorage
    {
        private readonly ThreadLocal<IStorageActionsAccessor> current = new ThreadLocal<IStorageActionsAccessor>();
        
        private readonly RavenConfiguration configuration;
        private readonly Action onCommit;
        private TableStorage tableStroage;
        private IPersistentSource persistenceSource;
        private bool disposed;
        private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();

        public TransactionalStorage(RavenConfiguration configuration, Action onCommit)
        {
            this.configuration = configuration;
            this.onCommit = onCommit;
        }

        public void Dispose()
        {
            disposerLock.EnterWriteLock();
            try
            {
                if (disposed)
                    return;
                persistenceSource.Dispose();
            }
            finally
            {
                disposed = true;
                disposerLock.ExitWriteLock();
            }
        }

        public Guid Id
        {
            get { throw new NotImplementedException(); }
        }

        [DebuggerNonUserCode]
        public void Batch(Action<IStorageActionsAccessor> action)
        {
            if (disposed)
            {
                Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.");
                return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
            }
            if(current.Value != null)
            {
                action(current.Value);
                return;
            }
            disposerLock.EnterReadLock();
            try
            {
                using (tableStroage.BeginTransaction())
                {
                    var storageActionsAccessor = new StorageActionsAccessor(tableStroage);
                    current.Value = storageActionsAccessor;
                    action(current.Value);
                    tableStroage.Commit();
                    storageActionsAccessor.InvokeOnCommit();
                    onCommit();
                }
            }
            finally
            {
                disposerLock.ExitReadLock();
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
            if (configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction == false &&
                Directory.Exists(configuration.DataDirectory) == false)
                Directory.CreateDirectory(configuration.DataDirectory);

            persistenceSource = configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction
                          ? (IPersistentSource)new MemoryPersistentSource()
                          : new FileBasedPersistentSource(configuration.DataDirectory, "Raven", configuration.TransactionMode);

            tableStroage = new TableStorage(persistenceSource);

            tableStroage.Initialze();

            return persistenceSource.CreatedNew;
        }

        public void StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory)
        {
            throw new NotImplementedException();
        }

        public void Restore(string backupLocation, string databaseLocation)
        {
            throw new NotImplementedException();
        }

        public Type TypeForRunningQueriesInRemoteAppDomain
        {
            get { throw new NotImplementedException(); }
        }

        public object StateForRunningQueriesInRemoteAppDomain
        {
            get { throw new NotImplementedException(); }
        }
    }
}