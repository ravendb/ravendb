using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.DocumentsCompression;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.DocumentsCompression
{
    internal class ShardedDocumentsCompressionHandlerProcessorForPost : AbstractDocumentsCompressionHandlerProcessorForPost<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedDocumentsCompressionHandlerProcessorForPost([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
