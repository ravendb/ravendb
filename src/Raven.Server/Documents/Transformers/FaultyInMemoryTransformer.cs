using System;
using Raven.Client.Documents.Transformers;
using Raven.Server.Documents.Includes;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Transformers
{
    internal class FaultyInMemoryTransformer : Transformer
    {
        private readonly Exception _e;

        public FaultyInMemoryTransformer(string name, long etag, Exception e)
            : base(null, null, null)
        {
            _e = e;
            Name = name;
            Etag = etag;
        }

        public override long Etag { get; }

        public override string Name { get; }

        public override void SetLock(TransformerLockMode mode)
        {
            throw new NotSupportedException($"Transformer with name {Name} is in-memory implementation of a faulty transformer", _e);
        }

        public override TransformationScope OpenTransformationScope(BlittableJsonReaderObject parameters, IncludeDocumentsCommand include, DocumentsStorage documentsStorage, TransformerStore transformerStore, DocumentsOperationContext context, bool nested)
        {
            throw new NotSupportedException($"Transformer with name {Name} is in-memory implementation of a faulty transformer", _e);
        }
    }
}