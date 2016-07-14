using System;
using System.IO;
using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Imports.Newtonsoft.Json;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Static;
using Sparrow;
using Voron.Platform.Posix;

namespace Raven.Server.Documents.Transformers
{
    public class Transformer
    {
        public const string FileExtension = ".transformer";

        private static readonly ILog Log = LogManager.GetLogger(typeof(Transformer));

        private readonly TransformerBase _transformer;

        private readonly object _locker = new object();

        private IndexingConfiguration _configuration;

        private Transformer(int transformerId, TransformerDefinition definition, TransformerBase transformer)
        {
            Definition = definition;
            Definition.TransfomerId = transformerId;

            _transformer = transformer;
        }

        public int TransformerId => Definition.TransfomerId;

        public string Name => Definition?.Name;

        public readonly TransformerDefinition Definition;

        public void SetLock(TransformerLockMode mode)
        {
            if (Definition.LockMode == mode)
                return;

            lock (_locker)
            {
                if (Definition.LockMode == mode)
                    return;

                if (Log.IsDebugEnabled)
                    Log.Debug($"Changing lock mode for '{Name} ({TransformerId})' from '{Definition.LockMode}' to '{mode}'.");

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

        private void Initialize(IndexingConfiguration configuration)
        {
            lock (_locker)
            {
                _configuration = configuration;
                Persist();
            }
        }

        private void Persist()
        {
            if (_configuration.RunInMemory)
                return;

            File.WriteAllText(GetPath(TransformerId, _configuration), JsonConvert.SerializeObject(Definition, Formatting.Indented, Default.Converters));
        }

        public static Transformer CreateNew(int transformerId, TransformerDefinition definition, IndexingConfiguration configuration)
        {
            var compiledTransformer = IndexAndTransformerCompilationCache.GetTransformerInstance(definition);
            var transformer = new Transformer(transformerId, definition, compiledTransformer);
            transformer.Initialize(configuration);

            return transformer;
        }

        public static Transformer Open(int transformerId, IndexingConfiguration configuration)
        {
            var path = GetPath(transformerId, configuration);
            if (File.Exists(path) == false)
                throw new InvalidOperationException($"Could not find transformer file at '{path}'.");

            var transformerDefinitionAsText = File.ReadAllText(path);
            var transformerDefinition = JsonConvert.DeserializeObject<TransformerDefinition>(transformerDefinitionAsText);

            return CreateNew(transformerId, transformerDefinition, configuration);
        }

        private static string GetPath(int transformerId, IndexingConfiguration configuration)
        {
            var path = Path.Combine(configuration.IndexStoragePath, "Transformers", transformerId + FileExtension);

            if (Platform.RunningOnPosix)
                path = PosixHelper.FixLinuxPath(path);

            return path;
        }
    }
}