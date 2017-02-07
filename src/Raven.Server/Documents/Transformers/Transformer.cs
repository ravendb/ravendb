using System;
using System.IO;
using System.Text;
using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Imports.Newtonsoft.Json;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform;
using Voron.Platform.Posix;

namespace Raven.Server.Documents.Transformers
{
    public class Transformer
    {
        public const string FileExtension = ".transformer";

        private readonly TransformerBase _transformer;
        private readonly Logger _log;

        private readonly object _locker = new object();

        private IndexingConfiguration _configuration;

        protected Transformer(int transformerId, TransformerDefinition definition, TransformerBase transformer, Logger log)
        {
            Definition = definition;

            if (Definition != null) // FaultyInMemoryTransformer can have this
                Definition.TransfomerId = transformerId;

            _transformer = transformer;
            _log = log;
        }

        public virtual int TransformerId => Definition.TransfomerId;

        public virtual string Name => Definition?.Name;

        public virtual int Hash => Definition?.GetHashCode() ?? TransformerId;

        public virtual bool HasLoadDocument => _transformer.HasLoadDocument;

        public virtual bool HasTransformWith => _transformer.HasTransformWith;

        public virtual bool HasGroupBy => _transformer.HasGroupBy;

        public virtual bool HasInclude => _transformer.HasInclude;

        public bool MightRequireTransaction => HasLoadDocument || HasInclude || HasTransformWith;

        public readonly TransformerDefinition Definition;

        public virtual void SetLock(TransformerLockMode mode)
        {
            if (Definition.LockMode == mode)
                return;

            lock (_locker)
            {
                if (Definition.LockMode == mode)
                    return;

                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Changing lock mode for '{Name} ({TransformerId})' from '{Definition.LockMode}' to '{mode}'.");

                var oldMode = Definition.LockMode;
                try
                {
                    Definition.LockMode = mode;
                    Persist();
                }
                catch (Exception)
                {
                    Definition.LockMode = oldMode;
                    throw;
                }
            }
        }

        private void Initialize(IndexingConfiguration configuration, bool persist)
        {
            lock (_locker)
            {
                _configuration = configuration;

                if (persist)
                    Persist();
            }
        }

        private void Persist()
        {
            if (_configuration.RunInMemory)
                return;

            File.WriteAllText(GetPath(TransformerId, Name, _configuration).FullPath, JsonConvert.SerializeObject(Definition, Formatting.Indented, Default.Converters));
        }

        public static Transformer CreateNew(int transformerId, TransformerDefinition definition,
            IndexingConfiguration configuration, Logger log)
        {
            var compiledTransformer = IndexAndTransformerCompilationCache.GetTransformerInstance(definition);
            var transformer = new Transformer(transformerId, definition, compiledTransformer, log);
            transformer.Initialize(configuration, persist: true);

            return transformer;
        }

        public static Transformer Open(int transformerId, string fullPath, IndexingConfiguration configuration, Logger log)
        {
            if (File.Exists(fullPath) == false)
                throw new InvalidOperationException($"Could not find transformer file at '{fullPath}'.");

            var transformerDefinitionAsText = File.ReadAllText(fullPath);
            var transformerDefinition = JsonConvert.DeserializeObject<TransformerDefinition>(transformerDefinitionAsText);

            if (transformerDefinition == null)
                throw new InvalidOperationException($"Could not read transformer definition from '{fullPath}'.");

            var compiledTransformer = IndexAndTransformerCompilationCache.GetTransformerInstance(transformerDefinition);
            var transformer = new Transformer(transformerId, transformerDefinition, compiledTransformer, log);
            transformer.Initialize(configuration, persist: false);

            return transformer;
        }

        private static PathSetting GetPath(int transformerId, string name, IndexingConfiguration configuration)
        {
            return
                configuration.StoragePath.Combine(Path.Combine("Transformers",
                    $"{transformerId}.{Convert.ToBase64String(Encoding.UTF8.GetBytes(name))}{FileExtension}"));
        }

        public virtual TransformationScope OpenTransformationScope(BlittableJsonReaderObject parameters, IncludeDocumentsCommand include, DocumentsStorage documentsStorage, TransformerStore transformerStore, DocumentsOperationContext context, bool nested = false)
        {
            return new TransformationScope(_transformer, parameters, include, documentsStorage, transformerStore, context, nested);
        }

        public static bool TryReadIdFromFile(string name, out int transformerId)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                transformerId = -1;
                return false;
            }

            var indexOfDot = name.IndexOf(".", StringComparison.OrdinalIgnoreCase);
            name = name.Substring(0, indexOfDot);

            return int.TryParse(name, out transformerId);
        }

        public static string TryReadNameFromFile(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return null;

            var parts = name.Split('.');
            if (parts.Length != 3)
                return null;

            var encodedName = parts[1];

            try
            {
                return Encoding.UTF8.GetString(Convert.FromBase64String(encodedName));
            }
            catch (Exception)
            {
                return null;
            }
        }

        public void Delete()
        {
            if (_configuration.RunInMemory)
                return;

            var path = GetPath(TransformerId, Name, _configuration);
            if (File.Exists(path.FullPath) == false)
                return;

            File.Delete(path.FullPath);
        }
    }
}