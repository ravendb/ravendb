using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Exceptions.Transformers;
using Raven.Client.Documents.Transformers;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections.LockFree;
using Sparrow.Logging;

namespace Raven.Server.Documents.Transformers
{
    public class TransformerStore
    {
        private readonly Logger _log;

        private readonly DocumentDatabase _documentDatabase;
        private readonly ServerStore _serverStore;

        private readonly ConcurrentDictionary<string, Transformer> _transformers = new ConcurrentDictionary<string, Transformer>();

        /// <summary>
        /// The current lock, used to make sure indexes/transformers have a unique names
        /// </summary>
        private readonly object _indexAndTransformerLocker;

        private bool _initialized;
        
        public TransformerStore(DocumentDatabase documentDatabase, ServerStore serverStore, object indexAndTransformerLocker)
        {
            _documentDatabase = documentDatabase;
            _serverStore = serverStore;
            _log = LoggingSource.Instance.GetLogger<TransformerStore>(_documentDatabase.Name);
            _indexAndTransformerLocker = indexAndTransformerLocker;

            if (_serverStore != null)
            {
                _serverStore.Cluster.DatabaseChanged += HandleDatabaseRecordChange;
            }
        }

        public void Initialize(DatabaseRecord record)
        {
            if (_initialized)
                throw new InvalidOperationException($"{nameof(TransformerStore)} was already initialized.");

            lock (_indexAndTransformerLocker)
            {
                
                if (_initialized)
                    throw new InvalidOperationException($"{nameof(TransformerStore)} was already initialized.");

                _initialized = true;

                OpenTransformers(record);
            }
        }

        private void HandleDatabaseRecordChange(object sender, string changedDatabase)
        {
            if (_serverStore == null)
                return;
            if (string.Equals(changedDatabase, _documentDatabase.Name, StringComparison.OrdinalIgnoreCase) == false)
                return;
            
            var transformersToDelete = new List<string>();

            TransactionOperationContext context;
            using (_serverStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                var databaseRecord = _serverStore.Cluster.ReadDatabase(context, _documentDatabase.Name);
                if (databaseRecord == null)
                    return;

                try
                {
                    lock (_indexAndTransformerLocker)
                    {
                        foreach (var existingTransformer in _transformers.Values)
                        {
                            if (databaseRecord.Transformers.ContainsKey(existingTransformer.Name) == false)
                            {
                                transformersToDelete.Add(existingTransformer.Name);
                            }
                        }

                        foreach (var transformerInRecord in databaseRecord.Transformers)
                        {
                            Transformer existingTransformer;
                            _transformers.TryGetValue(transformerInRecord.Key, out existingTransformer);

                            if (existingTransformer != null &&
                                transformerInRecord.Value.Equals(existingTransformer.Definition))
                            {
                                continue;
                            }

                            if (existingTransformer != null)
                                DeleteTransformer(existingTransformer.Name);

                            CreateTransformer(transformerInRecord.Value);
                        }

                        foreach (var transformerToDelete in transformersToDelete)
                        {
                            DeleteTransformer(transformerToDelete);
                        }

                        if (_log.IsInfoEnabled)
                            _log.Info("Transformers  configuration changed");
                    }
                }
                catch (Exception e)
                {
                    //TODO: This should generate an alert, so admin will know that something is very bad
                    //TODO: Or this should throw and we should have a config flag to ignore the error
                    if (_log.IsOperationsEnabled)
                        _log.Operations(
                            $"Cannot enable versioning for documents as the versioning configuration in the database record is missing or not valid: {databaseRecord}",
                            e);
                }
            }
        }

        private void OpenTransformers(DatabaseRecord record)
        {
            if (_documentDatabase.Configuration.Indexing.RunInMemory)
                return;

            lock (_indexAndTransformerLocker)
            {
                foreach (var transformerDefinition in record.Transformers.Values)
                {
                    if (_documentDatabase.DatabaseShutdown.IsCancellationRequested)
                        return;
                    
                    var transformerName = transformerDefinition.Name;
                    

                    List<Exception> exceptions = null;
                    if (_documentDatabase.Configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)
                        exceptions = new List<Exception>();

                    try
                    {
                        var transformer = Transformer.Open(transformerName, LoggingSource.Instance.GetLogger<Transformer>(_documentDatabase.Name), record);
                        _transformers.Add(transformer.Name,transformer);
                    }
                    catch (Exception e)
                    {
                        exceptions?.Add(e);

                        var fakeTransformer = new FaultyInMemoryTransformer(transformerDefinition.Name, e);

                        var message = $"Could not open transformer with name {transformerName}. Created in-memory, fake instance: {fakeTransformer.Name}";

                        if (_log.IsOperationsEnabled)
                            _log.Operations(message, e);

                        _documentDatabase.NotificationCenter.Add(AlertRaised.Create("Transformers store initialization error",
                            message,
                            AlertType.TransformerStore_TransformerCouldNotBeOpened,
                            NotificationSeverity.Error,
                            key: fakeTransformer.Name,
                            details: new ExceptionDetails(e)));

                        _transformers.Add(fakeTransformer.Name,fakeTransformer);
                    }

                    if (exceptions != null && exceptions.Count > 0)
                        throw new AggregateException("Could not load some of the transformers", exceptions);
                }
            }
        }

        public void CreateTransformer(TransformerDefinition definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));
            
            var index = _documentDatabase.IndexStore.GetIndex(definition.Name);
            if (index != null)
            {
                throw new IndexOrTransformerAlreadyExistException($"Tried to create a transformer with a name of {definition.Name}, but an index under the same name exist");
            }
                
            var transformer = Transformer.CreateNew(definition, LoggingSource.Instance.GetLogger<Transformer>(_documentDatabase.Name));
            CreateTransformerInternal(transformer);
            
        }

        public Transformer GetTransformer(string name)
        {
            Transformer transformer;
            if (_transformers.TryGetValue(name, out transformer) == false)
                return null;

            return transformer;
        }

        public bool TryDeleteTransformerIfExists(string name)
        {
            var transformer = GetTransformer(name);
            if (transformer == null)
                return false;

            DeleteTransformerInternal(transformer.Name);
            return true;
        }


        public void DeleteTransformer(string name)
        {
            var transformer = GetTransformer(name);
            if (transformer == null)
                TransformerDoesNotExistException.ThrowFor(name);

            DeleteTransformerInternal(transformer.Name);
        }

        public IEnumerable<Transformer> GetTransformers()
        {
            return _transformers.Values;
        }

        public int GetTransformersCount()
        {
            return _transformers.Count;
        }

        private void DeleteTransformerInternal(string name)
        {
            lock (_indexAndTransformerLocker)
            {
                Transformer transformer;
                if (_transformers.TryRemove(name, out transformer) == false)
                    TransformerDoesNotExistException.ThrowFor(name);
                
                //var tombstoneEtag = _documentDatabase.IndexMetadataPersistence.OnTransformerDeleted(transformer);
                _documentDatabase.Changes.RaiseNotifications(new TransformerChange
                {
                    Name = transformer.Name,
                    Type = TransformerChangeTypes.TransformerRemoved,
                  //  Etag = tombstoneEtag
                });
            }
        }

        private void CreateTransformerInternal(Transformer transformer)
        {
            Debug.Assert(transformer != null);
            Debug.Assert(string.IsNullOrWhiteSpace(transformer.Name?.Trim())==false);
            
            _transformers.Add(transformer.Name,transformer);
            _documentDatabase.Changes.RaiseNotifications(new TransformerChange
            {
                Name = transformer.Name,
                Type = TransformerChangeTypes.TransformerAdded
            });
        }
 
    }
}