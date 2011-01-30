//-----------------------------------------------------------------------
// <copyright file="TransactionalStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Munin;
using Raven.Storage.Managed.Backup;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
    public class TransactionalStorage : ITransactionalStorage
    {
        private readonly ThreadLocal<IStorageActionsAccessor> current = new ThreadLocal<IStorageActionsAccessor>();

        private readonly InMemoryRavenConfiguration configuration;
        private readonly Action onCommit;
        private TableStorage tableStroage;
        private IPersistentSource persistenceSource;
        private bool disposed;
        private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();
        private Timer idleTimer;
        private long lastUsageTime;
        private IUuidGenerator uuidGenerator;

        [ImportMany]
        public IEnumerable<AbstractDocumentCodec> DocumentCodecs { get; set; }

        public TransactionalStorage(InMemoryRavenConfiguration configuration, Action onCommit)
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
                if (idleTimer != null)
                    idleTimer.Dispose();
                if (persistenceSource != null)
                    persistenceSource.Dispose();
				if(tableStroage != null)
					tableStroage.Dispose();
            }
            finally
            {
                disposed = true;
                disposerLock.ExitWriteLock();
            }
        }

        public Guid Id
        {
            get; private set;
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
                Interlocked.Exchange(ref lastUsageTime, DateTime.Now.ToBinary());
                using (tableStroage.BeginTransaction())
                {
                    var storageActionsAccessor = new StorageActionsAccessor(tableStroage, uuidGenerator, DocumentCodecs);
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

        public bool Initialize(IUuidGenerator generator)
        {
            uuidGenerator = generator;
            if (configuration.RunInMemory  == false && Directory.Exists(configuration.DataDirectory) == false)
                Directory.CreateDirectory(configuration.DataDirectory);

            persistenceSource = configuration.RunInMemory
                          ? (IPersistentSource)new MemoryPersistentSource()
                          : new FileBasedPersistentSource(configuration.DataDirectory, "Raven", configuration.TransactionMode == TransactionMode.Safe);

            tableStroage = new TableStorage(persistenceSource);

            idleTimer = new Timer(MaybeOnIdle, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

            tableStroage.Initialze();

            if(persistenceSource.CreatedNew)
            {
                Id = Guid.NewGuid();
                Batch(accessor => tableStroage.Details.Put("id", Id.ToByteArray()));
            }
            else
            {
                var readResult = tableStroage.Details.Read("id");
                Id = new Guid(readResult.Data());
            }

            return persistenceSource.CreatedNew;
        }

        public void StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory)
        {
            var backupOperation = new BackupOperation(database, persistenceSource, database.Configuration.DataDirectory, backupDestinationDirectory);
            ThreadPool.QueueUserWorkItem(backupOperation.Execute);
		
        }

        public void Restore(string backupLocation, string databaseLocation)
        {
            new RestoreOperation(backupLocation, databaseLocation).Execute();
        }

        public Type TypeForRunningQueriesInRemoteAppDomain
        {
            get { return typeof(RemoteManagedStorage); }
        }

        public object StateForRunningQueriesInRemoteAppDomain
        {
            get { return persistenceSource.CreateRemoteAppDomainState(); }
        }

    	public string FriendlyName
    	{
			get { return "Munin"; }
    	}

    	public bool HandleException(Exception exception)
        {
            return false;
        }

        private void MaybeOnIdle(object _)
        {
            var ticks = Interlocked.Read(ref lastUsageTime);
            var lastUsage = DateTime.FromBinary(ticks);
            if ((DateTime.Now - lastUsage).TotalSeconds < 30)
                return;

            tableStroage.PerformIdleTasks();
        }

    	public void EnsureCapacity(int value)
    	{
    		persistenceSource.EnsureCapacity(value);
    	}
    }
}