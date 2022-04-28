using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Processors.DocumentsCompression;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.DocumentsCompression
{
    internal class ShardedDocumentsCompressionHandlerProcessorForGet : AbstractDocumentsCompressionHandlerProcessorForGet<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedDocumentsCompressionHandlerProcessorForGet([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override DocumentsCompressionConfiguration GetDocumentsCompressionConfiguration()
        {
            return RequestHandler.DatabaseContext.DatabaseRecord.DocumentsCompression;
        }
    }
}
