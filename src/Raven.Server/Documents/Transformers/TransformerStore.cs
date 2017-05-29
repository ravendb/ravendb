using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Exceptions.Transformers;
using Raven.Client.Documents.Transformers;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Server;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Transformers;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.Transformers
{
    public class TransformerStore : IDisposable
    {
        private readonly Logger _log;

        private readonly DocumentDatabase _documentDatabase;
        private readonly ServerStore _serverStore;

        private readonly CollectionOfTransformers _transformers = new CollectionOfTransformers();

        /// <summary>
        /// The current lock, used to make sure indexes/transformers have a unique names
        /// </summary>
        private readonly object _locker = new object();

        private readonly SemaphoreSlim _indexAndTransformerLocker;

        private bool _initialized;

        public TransformerStore(DocumentDatabase documentDatabase, ServerStore serverStore, SemaphoreSlim indexAndTransformerLocker)
        {
            _documentDatabase = documentDatabase;
            _serverStore = serverStore;
            _log = LoggingSource.Instance.GetLogger<TransformerStore>(_documentDatabase.Name);
            _indexAndTransformerLocker = indexAndTransformerLocker;
        }

        public void HandleDatabaseRecordChange()
        {
            try
            {
                TransactionOperationContext context;
                using (_serverStore.ContextPool.AllocateOperationContext(out context))
                {
                    DatabaseRecord record;
                    using (context.OpenReadTransaction())
                    {
                        record = _serverStore.Cluster.ReadDatabase(context, _documentDatabase.Name);
                        if (record == null)
                            return;
                    }

                    lock (_locker)
                    {
                        HandleDeletes(record);
                        HandleChanges(record);
                    }
                }
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Could not proccess database change for TransformerStore",e);
            }
        }

        private void HandleChanges(DatabaseRecord record)
        {
            foreach (var kvp in record.Transformers)
            {
                var name = kvp.Key;
                var definition = kvp.Value;

                var existingTransformer = GetTransformer(name);
                if (existingTransformer != null)
                {
                    var result = definition.Compare(existingTransformer.Definition);
                    result &= ~TransformerDefinitionCompareDifferences.Etag;

                    if (result == TransformerDefinitionCompareDifferences.None)
                        continue;

                    if (result.HasFlag(TransformerDefinitionCompareDifferences.LockMode))
                    {
                        existingTransformer.Definition.LockMode = definition.LockMode;
                        continue;
                    }

                    DeleteTransformerInternal(existingTransformer);
                }

                try
                {
                    var transformer = Transformer.CreateNew(definition, _documentDatabase.Configuration.Indexing, LoggingSource.Instance.GetLogger<Transformer>(_documentDatabase.Name));
                    CreateTransformerInternal(transformer);
                }
                catch (Exception e)
                {
                    var fakeTransformer = new FaultyInMemoryTransformer(kvp.Key, definition.Etag, e);

                    var message = $"Could not create transformer with etag {definition.Etag}. Created in-memory, fake instance: {fakeTransformer.Name}";

                    if (_log.IsOperationsEnabled)
                        _log.Operations(message, e);

                    _documentDatabase.NotificationCenter.Add(AlertRaised.Create("Transformer creation failed",
                        message,
                        AlertType.TransformerStore_TransformerCouldNotBeCreated,
                        NotificationSeverity.Error,
                        key: fakeTransformer.Name,
                        details: new ExceptionDetails(e)));

                    CreateTransformerInternal(fakeTransformer);
                }
            }
        }

        private void HandleDeletes(DatabaseRecord record)
        {
            foreach (var transformer in _transformers)
            {
                if (record.Transformers.ContainsKey(transformer.Name))
                    continue;

                DeleteTransformerInternal(transformer);
            }
        }

        public Task InitializeAsync(DatabaseRecord record)
        {
            if (_initialized)
                throw new InvalidOperationException($"{nameof(TransformerStore)} was already initialized.");

            lock (_locker)
            {
                if (_initialized)
                    throw new InvalidOperationException($"{nameof(TransformerStore)} was already initialized.");

                _initialized = true;

                return Task.Factory.StartNew(() => OpenTransformers(record), TaskCreationOptions.LongRunning);
            }
        }

        private void OpenTransformers(DatabaseRecord record)
        {
            if (_documentDatabase.Configuration.Indexing.RunInMemory)
                return;

            lock (_locker)
            {
                foreach (var kvp in record.Transformers)
                {
                    if (_documentDatabase.DatabaseShutdown.IsCancellationRequested)
                        return;

                    var definition = kvp.Value;

                    List<Exception> exceptions = null;
                    if (_documentDatabase.Configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)
                        exceptions = new List<Exception>();

                    try
                    {
                        var transformer = Transformer.CreateNew(definition, _documentDatabase.Configuration.Indexing, LoggingSource.Instance.GetLogger<Transformer>(_documentDatabase.Name));
                        _transformers.Add(transformer);
                    }
                    catch (Exception e)
                    {
                        exceptions?.Add(e);

                        var fakeTransformer = new FaultyInMemoryTransformer(kvp.Key, definition.Etag, e);

                        var message = $"Could not create transformer with etag {definition.Etag}. Created in-memory, fake instance: {fakeTransformer.Name}";

                        if (_log.IsOperationsEnabled)
                            _log.Operations(message, e);

                        _documentDatabase.NotificationCenter.Add(AlertRaised.Create("Transformer creation failed",
                            message,
                            AlertType.TransformerStore_TransformerCouldNotBeCreated,
                            NotificationSeverity.Error,
                            key: fakeTransformer.Name,
                            details: new ExceptionDetails(e)));

                        _transformers.Add(fakeTransformer);
                    }

                    if (exceptions != null && exceptions.Count > 0)
                        throw new AggregateException("Could not load some of the transformers", exceptions);
                }
            }
        }

        public async Task<long> CreateTransformer(TransformerDefinition definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            IndexAndTransformerCompilationCache.GetTransformerInstance(definition); // pre-compile it and validate

            await _indexAndTransformerLocker.WaitAsync();

            try
            {
                var command = new PutTransformerCommand(definition, _documentDatabase.Name);

                try
                {
                    var (index, result) = await _serverStore.SendToLeaderAsync(command);

                    await _documentDatabase.WaitForIndexNotification(index);

                    var instance = GetTransformer(definition.Name);
                    return instance.Etag;
                }
                catch (CommandExecutionException e)
                {
                    throw e.InnerException;
                }
            }
            finally
            {
                _indexAndTransformerLocker.Release();
            }
        }

        public Transformer GetTransformer(int id)
        {
            Transformer transformer;
            if (_transformers.TryGetByEtag(id, out transformer) == false)
                return null;

            return transformer;
        }

        public Transformer GetTransformer(string name)
        {
            Transformer transformer;
            if (_transformers.TryGetByName(name, out transformer) == false)
                return null;

            return transformer;
        }

        public async Task<bool> TryDeleteTransformerIfExists(string name)
        {
            await _indexAndTransformerLocker.WaitAsync();

            try
            {
                var transformer = GetTransformer(name);
                if (transformer == null)
                    return false;

                var (etag, result) = await _serverStore.SendToLeaderAsync(new DeleteTransformerCommand(transformer.Name, _documentDatabase.Name));

                await _documentDatabase.WaitForIndexNotification(etag);

                return true;
            }
            finally
            {
                _indexAndTransformerLocker.Release();
            }
        }

        public async Task DeleteTransformer(string name)
        {
            await _indexAndTransformerLocker.WaitAsync();

            try
            {
                var transformer = GetTransformer(name);
                if (transformer == null)
                    TransformerDoesNotExistException.ThrowFor(name);

                var (etag, result) = await _serverStore.SendToLeaderAsync(new DeleteTransformerCommand(transformer.Name, _documentDatabase.Name));

                await _documentDatabase.WaitForIndexNotification(etag);
            }
            finally
            {
                _indexAndTransformerLocker.Release();
            }
        }

        public IEnumerable<Transformer> GetTransformers()
        {
            return _transformers;
        }

        public int GetTransformersCount()
        {
            return _transformers.Count;
        }

        public async Task SetLock(string name, TransformerLockMode mode)
        {
            await _indexAndTransformerLocker.WaitAsync();

            try
            {
                var transformer = GetTransformer(name);
                if (transformer == null)
                    TransformerDoesNotExistException.ThrowFor(name);

                var faultyInMemoryTransformer = transformer as FaultyInMemoryTransformer;
                if (faultyInMemoryTransformer != null)
                {
                    faultyInMemoryTransformer.SetLock(mode); // this will throw proper exception
                    return;
                }

                var command = new SetTransformerLockCommand(name, mode, _documentDatabase.Name);

                var (etag, result) = await _serverStore.SendToLeaderAsync(command);

                await _documentDatabase.WaitForIndexNotification(etag);
            }
            finally
            {
                _indexAndTransformerLocker.Release();
            }
        }

        public async Task Rename(string name, string newName)
        {
            await _indexAndTransformerLocker.WaitAsync();

            try
            {
                var transformer = GetTransformer(name);
                if (transformer == null)
                    TransformerDoesNotExistException.ThrowFor(name);

                var command = new RenameTransformerCommand(name, newName, _documentDatabase.Name);

                var (etag, result) = await _serverStore.SendToLeaderAsync(command);

                await _documentDatabase.WaitForIndexNotification(etag);
            }
            finally
            {
                _indexAndTransformerLocker.Release();
            }
        }

        private void DeleteTransformerInternal(Transformer transformer)
        {
            lock (_locker)
            {
                Transformer _;
                _transformers.TryRemoveByEtag(transformer.Etag, out _);

                _documentDatabase.Changes.RaiseNotifications(new TransformerChange
                {
                    Name = transformer.Name,
                    Type = TransformerChangeTypes.TransformerRemoved,
                    Etag = transformer.Etag
                });
            }
        }

        private void CreateTransformerInternal(Transformer transformer)
        {
            Debug.Assert(transformer != null);
            Debug.Assert(transformer.Etag > 0);

            _transformers.Add(transformer);
            _documentDatabase.Changes.RaiseNotifications(new TransformerChange
            {
                Name = transformer.Name,
                Type = TransformerChangeTypes.TransformerAdded,
                Etag = transformer.Etag
            });
        }

        public void Dispose()
        {
            
        }
    }
}