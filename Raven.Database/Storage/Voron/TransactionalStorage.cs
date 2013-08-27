using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
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

namespace Raven.Storage.Voron
{
    public class TransactionalStorage : ITransactionalStorage
    {
        private readonly ThreadLocal<object> disableBatchNesting = new ThreadLocal<object>();

        private IUuidGenerator uuidGenerator;
        private readonly IDocumentCacher documentCacher;
        private OrderedPartCollection<AbstractDocumentCodec> DocumentCodecs { get; set; }
        private readonly DisposableAction exitLockDisposable;
        private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        private volatile bool disposed;
        private const string DATABASE_FILENAME_EXTENSION = ".voron";

        public TransactionalStorage(InMemoryRavenConfiguration configuration, Action onCommit)
        {
            exitLockDisposable = new DisposableAction(() => Monitor.Exit(this));
        }



        public void Dispose()
        {
            disposerLock.EnterWriteLock();
            try
            {
                if (disposed)
                    return;
                disposed = true;
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

        public void Batch(Action<IStorageActionsAccessor> action)
        {
            throw new NotImplementedException();
        }

        public void ExecuteImmediatelyOrRegisterForSynchronization(Action action)
        {
            throw new NotImplementedException();
        }

        public bool Initialize(IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs)
        {
            throw new NotImplementedException();
        }

        public void StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory, bool incrementalBackup,
            DatabaseDocument documentDatabase)
        {
            throw new NotImplementedException();
        }

        public void Restore(string backupLocation, string databaseLocation, Action<string> output, bool defrag)
        {
            throw new NotImplementedException();
        }

        public long GetDatabaseSizeInBytes()
        {
            throw new NotImplementedException();
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
            return false;
        }

        public void Compact(InMemoryRavenConfiguration configuration)
        {
            throw new NotImplementedException();
        }

        public Guid ChangeId()
        {
            throw new NotImplementedException();
        }

        public void ClearCaches()
        {
        }

        public void DumpAllStorageTables()
        {
            throw new NotImplementedException();
        }

        public InFlightTransactionalState GetInFlightTransactionalState(Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> put, Func<string, Etag, TransactionInformation, bool> delete)
        {
            throw new NotImplementedException();
        }

        public IList<string> ComputeDetailedStorageInformation()
        {
            throw new NotImplementedException();
        }
    }
}
