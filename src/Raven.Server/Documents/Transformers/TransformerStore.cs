using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Exceptions.Transformers;
using Raven.Client.Documents.Transformers;
using Raven.Server.Config.Settings;
using Raven.Server.Exceptions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Logging;

namespace Raven.Server.Documents.Transformers
{
    public class TransformerStore : IDisposable
    {
        private readonly Logger _log;

        private readonly DocumentDatabase _documentDatabase;

        private readonly CollectionOfTransformers _transformers = new CollectionOfTransformers();

        /// <summary>
        /// The current lock, used to make sure indexes/transformers have a unique names
        /// </summary>
        private readonly object _indexAndTransformerLocker;

        private bool _initialized;

        private PathSetting _path;

        public TransformerStore(DocumentDatabase documentDatabase, object indexAndTransformerLocker)
        {
            _documentDatabase = documentDatabase;
            _log = LoggingSource.Instance.GetLogger<TransformerStore>(_documentDatabase.Name);
            _indexAndTransformerLocker = indexAndTransformerLocker;
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
        }

        public int CreateTransformer(TransformerDefinition definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            lock (_indexAndTransformerLocker)
            {
                var index = _documentDatabase.IndexStore.GetIndex(definition.Name);
                if (index != null)
                {
                    throw new IndexOrTransformerAlreadyExistException($"Tried to create a transformer with a name of {definition.Name}, but an index under the same name exist");
                }
                Transformer existingTransformer;
                var lockMode = ValidateTransformerDefinition(definition.Name, out existingTransformer);
                if (lockMode == TransformerLockMode.LockedIgnore)
                    return existingTransformer.TransformerId;

                if (existingTransformer != null)
                {
                    if (existingTransformer.Definition.Equals(definition))
                        return existingTransformer.TransformerId; // no op for the same transformer

                    DeleteTransformer(existingTransformer.TransformerId);
                }

                var transformerId = _transformers.GetNextIndexId();
                var transformer = Transformer.CreateNew(transformerId, definition, _documentDatabase.Configuration.Indexing, LoggingSource.Instance.GetLogger<Transformer>(_documentDatabase.Name));

                return CreateTransformerInternal(transformer, transformerId);
            }
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

        private int CreateTransformerInternal(Transformer transformer, int transformerId)
        {
            Debug.Assert(transformer != null);
            Debug.Assert(transformerId > 0);

            var etag = _documentDatabase.IndexMetadataPersistence.OnTransformerCreated(transformer);
            _transformers.Add(transformer);
            _documentDatabase.Changes.RaiseNotifications(new TransformerChange
            {
                Name = transformer.Name,
                Type = TransformerChangeTypes.TransformerAdded,
                Etag = etag
            });

            return transformerId;
        }

        private TransformerLockMode ValidateTransformerDefinition(string name, out Transformer existingTransformer)
        {
            if (_transformers.TryGetByName(name, out existingTransformer) == false)
                return TransformerLockMode.Unlock;

            return existingTransformer.Definition.LockMode;
        }

        public void Dispose()
        {
        }

        public void Rename(string oldTransformerName, string newTransformerName)
        {
            Transformer transformer;
            if (_transformers.TryGetByName(oldTransformerName, out transformer) == false)
                throw new InvalidOperationException($"Index {oldTransformerName} does not exist");

            lock (_indexAndTransformerLocker)
            {
                var index = _documentDatabase.IndexStore.GetIndex(newTransformerName);
                if (index != null)
                {
                    throw new IndexOrTransformerAlreadyExistException(
                        $"Cannot rename transformer to {newTransformerName} because an index having the same name already exists");
                }

                Transformer _;
                if (_transformers.TryGetByName(newTransformerName, out _))
                {
                    throw new IndexOrTransformerAlreadyExistException(
                        $"Cannot rename transformer to {newTransformerName} because a transformer having the same name already exists");
                }

                transformer.Rename(newTransformerName);
                _transformers.RenameTransformer(transformer, newTransformerName);
            }

            _documentDatabase.Changes.RaiseNotifications(new TransformerChange
            {
                Name = oldTransformerName,
                Type = TransformerChangeTypes.TransformerRenamed
            });
        }
    }
}