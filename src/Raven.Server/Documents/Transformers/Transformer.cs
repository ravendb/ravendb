using Raven.Client.Documents.Transformers;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Transformers
{
    public class Transformer
    {
        private readonly object _locker = new object();

        private readonly TransformerBase _transformer;
        private readonly Logger _log;

        protected Transformer(TransformerDefinition definition, TransformerBase transformer, Logger log)
        {
            Definition = definition;
            _transformer = transformer;
            _log = log;
        }

        public virtual long Etag => Definition.Etag;

        public virtual string Name => Definition?.Name;

        public virtual long Hash => Definition?.GetHashCode() ?? Name.GetHashCode();

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
                    _log.Info($"Changing lock mode for '{Name} ({Etag})' from '{Definition.LockMode}' to '{mode}'.");

                Definition.LockMode = mode;
            }
        }

        public static Transformer CreateNew(TransformerDefinition definition, IndexingConfiguration configuration, Logger log)
        {
            var compiledTransformer = IndexAndTransformerCompilationCache.GetTransformerInstance(definition);
            var transformer = new Transformer(definition, compiledTransformer, log);

            return transformer;
        }

        public virtual TransformationScope OpenTransformationScope(BlittableJsonReaderObject parameters, IncludeDocumentsCommand include, DocumentsStorage documentsStorage, TransformerStore transformerStore, DocumentsOperationContext context, bool nested = false)
        {
            return new TransformationScope(_transformer, parameters, include, documentsStorage, transformerStore, context, nested);
        }
    }
}