using System;
using Raven.Abstractions.Indexing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Transformers
{
    internal class FaultyInMemoryTransformer : Transformer
    {
        public FaultyInMemoryTransformer(int transformerId, string name)
            : base(transformerId, null, null, null)
        {
            TransformerId = transformerId;
            Name = name;
        }

        public override int TransformerId { get; }

        public override string Name { get; }

        public override void SetLock(TransformerLockMode mode)
        {
            throw new NotSupportedException($"Transformer with id {TransformerId} is in-memory implementation of a faulty transformer");
        }

        public override TransformationScope OpenTransformationScope(DocumentsStorage documentsStorage, DocumentsOperationContext context)
        {
            throw new NotSupportedException($"Transformer with id {TransformerId} is in-memory implementation of a faulty transformer");
        }
    }
}