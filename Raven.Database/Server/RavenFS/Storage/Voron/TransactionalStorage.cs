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

using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Server.RavenFS.Storage.Voron.Impl;

using Voron.Impl;

using VoronExceptions = Voron.Exceptions;

namespace Raven.Database.Server.RavenFS.Storage.Voron
{
    public class TransactionalStorage : ITransactionalStorage
    {
        private readonly string path;

        private readonly NameValueCollection settings;

        private readonly ThreadLocal<IStorageActionsAccessor> current = new ThreadLocal<IStorageActionsAccessor>();

        private volatile bool disposed;

        private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        private readonly BufferPool bufferPool;

        private TableStorage tableStorage;

        public TransactionalStorage(string path, NameValueCollection settings)
        {
            this.path = path;
            this.settings = settings;

            bufferPool = new BufferPool(2L * 1024 * 1024 * 1024, int.MaxValue); // 2GB max buffer size (voron limit)
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

        public string Database { get; private set; }

        public Guid Id { get; private set; }

        public bool Initialize()
        {
            bool runInMemory;
            bool.TryParse(settings["Raven/FileSystem/RunInMemory"], out runInMemory);

            var persistenceSource = runInMemory ? (IPersistenceSource)new MemoryPersistenceSource() : new MemoryMapPersistenceSource(path, settings);

            tableStorage = new TableStorage(persistenceSource, bufferPool);

            if (persistenceSource.CreatedNew)
            {
                Id = Guid.NewGuid();

                using (var writeIdBatch = new WriteBatch())
                {
                    tableStorage.Details.Add(writeIdBatch, "id", Id.ToByteArray());
                    tableStorage.Write(writeIdBatch);
                }
            }
            else
            {
                using (var snapshot = tableStorage.CreateSnapshot())
                {
                    var read = tableStorage.Details.Read(snapshot, "id", null);
                    if (read == null || read.Reader == null || read.Reader.Length == 0) //precaution - might prevent NRE in edge cases
                        throw new InvalidDataException("Failed to initialize Voron transactional storage. Possible data corruption.");

                    using (var stream = read.Reader.AsStream())
                    using (var reader = new BinaryReader(stream))
                    {
                        Id = new Guid(reader.ReadBytes((int)stream.Length));
                    }
                }
            }

            return persistenceSource.CreatedNew;
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
                    Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.");
                    return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
                }

                ExecuteBatch(action);
            }
            catch (Exception e)
            {
                if (disposed)
                {
                    Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.");
                    return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
                }

                if (e.InnerException is VoronExceptions.ConcurrencyException)
                    throw new ConcurrencyException("Concurrent modification to the same file are not allowed", e.InnerException);

                throw;
            }
            finally
            {
                disposerLock.ExitReadLock();
                current.Value = null;
            }
        }

        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
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
                    writeBatchRef.Value = new WriteBatch();
                    using (var storageActionsAccessor = new StorageActionsAccessor(tableStorage, writeBatchRef, snapshot, bufferPool))
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
    }
}