using System;
using System.IO;
using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Imports.Newtonsoft.Json;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Logging;
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

        private Transformer(int transformerId, TransformerDefinition definition, TransformerBase transformer, Logger log)
        {
            Definition = definition;
            Definition.TransfomerId = transformerId;

            _transformer = transformer;
            _log = log;
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

                if (_log.IsInfoEnabled)
                    _log.Info($"Changing lock mode for '{Name} ({TransformerId})' from '{Definition.LockMode}' to '{mode}'.");

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

        public static Transformer CreateNew(int transformerId, TransformerDefinition definition, IndexingConfiguration configuration, Logger log)
        {
            var compiledTransformer = IndexAndTransformerCompilationCache.GetTransformerInstance(definition);
            var transformer = new Transformer(transformerId, definition, compiledTransformer, log);
            transformer.Initialize(configuration);

            return transformer;
        }

        public static Transformer Open(int transformerId, IndexingConfiguration configuration, Logger log)
        {
            var path = GetPath(transformerId, configuration);
            if (File.Exists(path) == false)
                throw new InvalidOperationException($"Could not find transformer file at '{path}'.");

            var transformerDefinitionAsText = File.ReadAllText(path);
            var transformerDefinition = JsonConvert.DeserializeObject<TransformerDefinition>(transformerDefinitionAsText);

            return CreateNew(transformerId, transformerDefinition, configuration, log);
        }

        private static string GetPath(int transformerId, IndexingConfiguration configuration)
        {
            var path = Path.Combine(configuration.IndexStoragePath, "Transformers", transformerId + FileExtension);

            if (Platform.RunningOnPosix)
                path = PosixHelper.FixLinuxPath(path);

            return path;
        }

        public TransformationScope OpenTransformationScope(DocumentDatabase documentDatabase, DocumentsOperationContext context)
        {
            return new TransformationScope(_transformer.TransformResults, documentDatabase, context);
        }
    }
}