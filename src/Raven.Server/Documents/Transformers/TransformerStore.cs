using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Sparrow;

using Voron.Platform.Posix;

namespace Raven.Server.Documents.Transformers
{
    public class TransformerStore : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(TransformerStore));

        private readonly DocumentDatabase _documentDatabase;

        private readonly CollectionOfTransformers _transformers = new CollectionOfTransformers();

        private readonly object _locker = new object();

        private bool _initialized;

        private string _path;

        public TransformerStore(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
        }

        public Task InitializeAsync()
        {
            if (_initialized)
                throw new InvalidOperationException($"{nameof(TransformerStore)} was already initialized.");

            lock (_locker)
            {
                if (_initialized)
                    throw new InvalidOperationException($"{nameof(TransformerStore)} was already initialized.");

                if (_documentDatabase.Configuration.Indexing.RunInMemory == false)
                {
                    _path = Path.Combine(_documentDatabase.Configuration.Indexing.IndexStoragePath, "Transformers");

                    if (Platform.RunningOnPosix)
                        _path = PosixHelper.FixLinuxPath(_path);

                    if (Directory.Exists(_path) == false && _documentDatabase.Configuration.Indexing.RunInMemory == false)
                        Directory.CreateDirectory(_path);
                }

                _initialized = true;

                return Task.Factory.StartNew(OpenTransformers, TaskCreationOptions.LongRunning);
            }
        }

        private void OpenTransformers()
        {
            if (_documentDatabase.Configuration.Indexing.RunInMemory)
                return;

            lock (_locker)
            {
                foreach (var transformerFile in new DirectoryInfo(_path).GetFiles("*.transformer"))
                {
                    if (_documentDatabase.DatabaseShutdown.IsCancellationRequested)
                        return;

                    int transformerId;
                    if (int.TryParse(transformerFile.Name, out transformerId) == false)
                        continue;

                    var transformer = Transformer.Open(transformerId, _documentDatabase.Configuration.Indexing);
                    _transformers.Add(transformer);
                }
            }
        }

        public int CreateTransformer(TransformerDefinition definition)
        {
            if (definition == null)
                throw new ArgumentNullException(nameof(definition));

            lock (_locker)
            {
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
                var transformer = Transformer.CreateNew(transformerId, definition, _documentDatabase.Configuration.Indexing);

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

        public void DeleteTransformer(string name)
        {
            var transformer = GetTransformer(name);
            if (transformer == null)
                throw new InvalidOperationException("There is no transformer with name: " + name);

            DeleteTransformerInternal(transformer.TransformerId);
        }

        public void DeleteTransformer(int id)
        {
            DeleteTransformerInternal(id);
        }

        private void DeleteTransformerInternal(int id)
        {
            lock (_locker)
            {
                Transformer transformer;
                if (_transformers.TryRemoveById(id, out transformer) == false)
                    throw new InvalidOperationException("There is no transformer with id: " + id);

                _documentDatabase.Notifications.RaiseNotifications(new TransformerChangeNotification
                {
                    Name = transformer.Name,
                    Type = TransformerChangeTypes.TransformerRemoved
                });
            }
        }

        private int CreateTransformerInternal(Transformer transformer, int transformerId)
        {
            Debug.Assert(transformer != null);
            Debug.Assert(transformerId > 0);

            _transformers.Add(transformer);

            _documentDatabase.Notifications.RaiseNotifications(new TransformerChangeNotification { Name = transformer.Name, Type = TransformerChangeTypes.TransformerAdded });

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
    }
}