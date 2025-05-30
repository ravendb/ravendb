using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.DocumentsCompression;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.SchemaValidation
{
    internal sealed class ShardedSchemaValidationHandlerProcessorForPost : AbstractDocumentsCompressionHandlerProcessorForPost<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSchemaValidationHandlerProcessorForPost([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
