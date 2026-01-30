using JetBrains.Annotations;
using Raven.Client.Documents.Operations.SchemaValidation;
using Raven.Server.Documents.Handlers.Processors.DocumentsCompression;
using Raven.Server.Documents.Handlers.Processors.SchemaValidation;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.SchemaValidation
{
    internal sealed class ShardedSchemaValidationHandlerProcessorForGet : AbstractSchemaValidationHandlerProcessorForGet<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSchemaValidationHandlerProcessorForGet([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override SchemaValidationConfiguration GetSchemaValidationConfiguration()
        {
            return RequestHandler.DatabaseContext.DatabaseRecord.SchemaValidation;
        }
    }
}
