using System;
using Raven.Client.Documents.Transformers;
using Raven.Server.Documents.Includes;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Transformers
{
    internal class FaultyInMemoryTransformer : Transformer
    {
        public FaultyInMemoryTransformer(string name)
            : base(null, null, null)
        {
            Name = name;
        }

        public override string Name { get; }

        public override void SetLock(TransformerLockMode mode)
        {
            throw new NotSupportedException($"Transformer with name {Name} is in-memory implementation of a faulty transformer");
        }

        public override TransformationScope OpenTransformationScope(BlittableJsonReaderObject parameters, IncludeDocumentsCommand include, DocumentsStorage documentsStorage, TransformerStore transformerStore, DocumentsOperationContext context, bool nested)
        {
            throw new NotSupportedException($"Transformer with name {Name} is in-memory implementation of a faulty transformer");
        }
    }
}