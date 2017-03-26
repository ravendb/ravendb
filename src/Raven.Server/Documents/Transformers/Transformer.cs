using System;
using System.Linq;
using Raven.Client.Documents.Transformers;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Transformers
{
    public class Transformer
    {
        private readonly TransformerBase _transformer;
        private readonly Logger _log;
        private readonly object _locker = new object();

        protected Transformer(TransformerDefinition definition, TransformerBase transformer, Logger log)
        {
            Definition = definition;
            _transformer = transformer;
            _log = log;
        }

        public virtual string Name => Definition?.Name;

        public virtual long Hash => Definition?.GetHashCode()??Name.GetHashCode();

        public virtual bool HasLoadDocument => _transformer.HasLoadDocument;

        public virtual bool HasTransformWith => _transformer.HasTransformWith;

        public virtual bool HasGroupBy => _transformer.HasGroupBy;

        public virtual bool HasInclude => _transformer.HasInclude;

        public bool MightRequireTransaction => HasLoadDocument || HasInclude || HasTransformWith;

        public readonly TransformerDefinition Definition;

        public static Transformer CreateNew(TransformerDefinition definition, Logger log)
        {
            TransformerBase compiledTransformer;
            try
            {
                compiledTransformer = IndexAndTransformerCompilationCache.GetTransformerInstance(definition);
            }
            catch (Exception e)
            {
                return new FaultyInMemoryTransformer(definition.Name, e);
            }
            var transformer = new Transformer(definition, compiledTransformer, log);
            return transformer;
        }

        public static Transformer Open(string transformerName, Logger log, DatabaseRecord record)
        {
            var transformerDefinitions = record.Transformers.Values.Where(x=>x.Name== transformerName).ToList();
            
            if (transformerDefinitions.Count == 0)
                throw new InvalidOperationException($"Could not read transformer definition for name {transformerName}");

            var transformerDefinition = transformerDefinitions.First();
            var compiledTransformer = IndexAndTransformerCompilationCache.GetTransformerInstance(transformerDefinition);
            var transformer = new Transformer(transformerDefinition, compiledTransformer, log);
            return transformer;
        }

        public virtual TransformationScope OpenTransformationScope(BlittableJsonReaderObject parameters, IncludeDocumentsCommand include, DocumentsStorage documentsStorage, TransformerStore transformerStore, DocumentsOperationContext context, bool nested = false)
        {
            return new TransformationScope(_transformer, parameters, include, documentsStorage, transformerStore, context, nested);
        }
    }
}