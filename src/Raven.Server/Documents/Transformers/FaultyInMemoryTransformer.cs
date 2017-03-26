using System;
using Raven.Server.Documents.Includes;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Transformers
{
    internal class FaultyInMemoryTransformer : Transformer
    {
        private readonly Exception _error;

        public FaultyInMemoryTransformer(string name, Exception error)
            : base(null, null, null)
        {
            _error = error;
            Name = name;
        }

        public override string Name { get; }

        public Exception Error => _error;

        public override TransformationScope OpenTransformationScope(BlittableJsonReaderObject parameters, IncludeDocumentsCommand include, DocumentsStorage documentsStorage, TransformerStore transformerStore, DocumentsOperationContext context, bool nested)
        {
            throw new NotSupportedException($"Transformer with name {Name} is in-memory implementation of a faulty transformer", _error);
        }
    }
}