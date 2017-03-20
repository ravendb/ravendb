using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Exceptions.Transformers;
using Raven.Client.Documents.Transformers;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Versioning;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
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
        private readonly object _indexAndTransformerLocker;

        private bool _initialized;

        private PathSetting _path;

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

        public Task InitializeAsync()
        {
            if (_initialized)
                throw new InvalidOperationException($"{nameof(TransformerStore)} was already initialized.");

            lock (_indexAndTransformerLocker)
            {
                if (_initialized)
                    throw new InvalidOperationException($"{nameof(TransformerStore)} was already initialized.");

                if (_documentDatabase.Configuration.Indexing.RunInMemory == false)
                {
                    _path = _documentDatabase.Configuration.Indexing.StoragePath.Combine("Transformers");

                    if (Directory.Exists(_path.FullPath) == false && _documentDatabase.Configuration.Indexing.RunInMemory == false)
                        Directory.CreateDirectory(_path.FullPath);
                }

                _initialized = true;

                return Task.Factory.StartNew(OpenTransformers, TaskCreationOptions.LongRunning);
            }
        }

        private void HandleDatabaseRecordChange(object sender, string changedDatabase)
        {
            lock (_indexAndTransformerLocker)
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
                        foreach (var existingTransformer in _transformers)
                        {
                            if (databaseRecord.Transformers.ContainsKey(existingTransformer.Name) ==false)
                            {
                                transformersToDelete.Add(existingTransformer.Name);
                            }
                        }

                        foreach (var transformerInRecord in databaseRecord.Transformers)
                        {
                            Transformer existingTransformer;
                            _transformers.TryGetByName(transformerInRecord.Key, out existingTransformer);

                            if (existingTransformer != null && 
                                transformerInRecord.Value.TransfomerId == existingTransformer.TransformerId  &&  
                                transformerInRecord.Value.Equals(existingTransformer.Definition) )
                            {
                                continue;
                            }

                            if (existingTransformer != null)
                                DeleteTransformer(existingTransformer.TransformerId);

                            CreateTransformer(transformerInRecord.Value);
                        }

                        foreach (var transformerToDelete in transformersToDelete)
                        {
                            DeleteTransformer(transformerToDelete);
                        }
                    
                        if (_log.IsInfoEnabled)
                            _log.Info("Versioning configuration changed");
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
        }

        private void OpenTransformers()
        {
            if (_documentDatabase.Configuration.Indexing.RunInMemory)
                return;

            lock (_indexAndTransformerLocker)
            {
                foreach (var transformerFile in new DirectoryInfo(_path.FullPath).GetFiles())
                {
                    if (_documentDatabase.DatabaseShutdown.IsCancellationRequested)
                        return;

                    if (string.Equals(transformerFile.Extension, Transformer.FileExtension, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    int transformerId;
                    if (Transformer.TryReadIdFromFile(transformerFile.Name, out transformerId) == false)
                        continue;

                    List<Exception> exceptions = null;
                    if (_documentDatabase.Configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)
                        exceptions = new List<Exception>();

                    try
                    {
                        var transformer = Transformer.Open(transformerId, transformerFile.FullName, _documentDatabase.Configuration.Indexing, LoggingSource.Instance.GetLogger<Transformer>(_documentDatabase.Name));
                        _transformers.Add(transformer);
                    }
                    catch (Exception e)
                    {
                        exceptions?.Add(e);

                        var fakeTransformer = new FaultyInMemoryTransformer(transformerId, Transformer.TryReadNameFromFile(transformerFile.Name));

                        var message = $"Could not open transformer with id {transformerId}. Created in-memory, fake instance: {fakeTransformer.Name}";

                        if (_log.IsOperationsEnabled)
                            _log.Operations(message, e);

                        _documentDatabase.NotificationCenter.Add(AlertRaised.Create("Transformers store initialization error",
                            message,
                            AlertType.TransformerStore_TransformerCouldNotBeOpened,
                            NotificationSeverity.Error,
                            key: fakeTransformer.Name,
                            details: new ExceptionDetails(e)));

                        _transformers.Add(fakeTransformer);
                    }

                    if (exceptions != null && exceptions.Count > 0)
                        throw new AggregateException("Could not load some of the transformers", exceptions);
                }
            }

            HandleDatabaseRecordChange(null, _documentDatabase.Name);
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
                
            var transformer = Transformer.CreateNew(definition, _documentDatabase.Configuration.Indexing, LoggingSource.Instance.GetLogger<Transformer>(_documentDatabase.Name));
            CreateTransformerInternal(transformer);
            
        }

        public Transformer GetTransformer(int id)
        {
            Transformer transformer;
            if (_transformers.TryGetById(id, out transformer) == false)
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

        public bool TryDeleteTransformerIfExists(string name)
        {
            var transformer = GetTransformer(name);
            if (transformer == null)
                return false;

            DeleteTransformerInternal(transformer.TransformerId);
            return true;
        }


        public void DeleteTransformer(string name)
        {
            var transformer = GetTransformer(name);
            if (transformer == null)
                TransformerDoesNotExistException.ThrowFor(name);

            DeleteTransformerInternal(transformer.TransformerId);
        }

        public void DeleteTransformer(int id)
        {
            DeleteTransformerInternal(id);
        }

        public IEnumerable<Transformer> GetTransformers()
        {
            return _transformers;
        }

        public int GetTransformersCount()
        {
            return _transformers.Count;
        }

        private void DeleteTransformerInternal(int id)
        {
            lock (_indexAndTransformerLocker)
            {
                Transformer transformer;
                if (_transformers.TryRemoveById(id, out transformer) == false)
                    TransformerDoesNotExistException.ThrowFor(id);

                transformer.Delete();
                var tombstoneEtag = _documentDatabase.IndexMetadataPersistence.OnTransformerDeleted(transformer);
                _documentDatabase.Changes.RaiseNotifications(new TransformerChange
                {
                    Name = transformer.Name,
                    Type = TransformerChangeTypes.TransformerRemoved,
                    Etag = tombstoneEtag
                });
            }
        }

        private void CreateTransformerInternal(Transformer transformer)
        {
            Debug.Assert(transformer != null);
            Debug.Assert(transformer.TransformerId > 0);

            var etag = _documentDatabase.IndexMetadataPersistence.OnTransformerCreated(transformer);
            _transformers.Add(transformer);
            _documentDatabase.Changes.RaiseNotifications(new TransformerChange
            {
                Name = transformer.Name,
                Type = TransformerChangeTypes.TransformerAdded,
                Etag = etag
            });
        }

        public void Dispose()
        {
        }

        public void Rename(string oldTransformerName, string newTransformerName)
        {
            throw new NotImplementedException();
        }
    }
}